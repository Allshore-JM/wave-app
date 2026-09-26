# Overlays — G10 adversarial review (the animation, plan section 21 phase C) — 2026-09-25

Gate G10 of plan section 21, before the client production merge. Phase C adds the Animation checkbox: chevrons gliding along
the dominant swell direction under wave height and peak period, wind particles under wind speed, fed by the `pdir` / `wdir`
frames of Phase B through the frame scheduler. Branch `feat/overlays-anim` (asset 2.8.0) off production 32017c6.

Three fresh-context reviewers at MAX effort (Fable 5.1), no access to the author's reasoning:
A — scheduler races and resource bounds (code + Node harness, before the first run with direction data);
B — direction data and drawing correctness on the live bucket; C — owner requirements, performance and phones on the
test site. Records of A's method: scratch `g10a/` (43 staged tests, a 750-run seeded race fuzz, 62 mutants).

## Reviewer A (27ebca3): 0 P0, 1 P1, 3 P2, 8 P3 — all fixed @ b4e20fc

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | A pending seek / step was stranded: `setAnim` and the new direction-resolution branch of `_checkRes` called `_prefetch` (planned around the frame on the map) and `_startDir` (ranked the same way), which aborted the pending target's field fetch by three routes (outside the ring; ranked "worst"; untick). The panel then said "loading +N h…" with nothing in flight until the next user action. Reproduced S1 (far seek + zoom 7→7.6), S2 (seek past the ring + tick), S5 (eviction) and fuzz seeds 74/371. | Fixed: `_anchorIndex()` = the pending target (else the frame on the map); `_prefetch` and `_startDir` plan and rank around it and never evict its field or direction fetch; `_syncFlow` fetches the shown frame's direction only when no other target is pending; untick aborts the direction fetches directly. Tests: S1, S2/S5 ports, untick-while-loading. Reviewer A's own 43 tests pass against the fix. |
| P2-1 | P2 | After a field switch `mount()` keeps `frameIndex` while the layer is empty, and the direction (smaller, lands first) was delivered on `frameIndex === idx`; the animator held a direction with no anchors and a tab show then threw a TypeError inside the animation frame (loop dead until the next rebuild). | Fixed: delivery requires `layer.hasFrame() && layer.entry === m.frames[idx]` (the layer shows that step of that run); `_start` / `_render*` guard a half-built animator. Test: D1 port. |
| P2-2 | P2 | A zoom that ended while the tab was hidden (zoomstart, movestart, hidden, zoomend, moveend, shown) lost its rebuild: the loop resumed on the pre-zoom anchors and canvas transform. | Fixed: nested suspends remember a requested rebuild (dirty flag) and the last resume runs it; the visibility handler resumes with a rebuild. Test: A1 port. |
| P2-3 | P2 | Coverage: `flow.test.js` was not in CI; 32 of 62 single mutants survived the repo's tests (a shown direction not cleared on a step change, the wind north/south sign, the in-flight room arithmetic, arrows over land, no restart after a tab switch, the trail fade sign, the animator's field on a switch). | Fixed: CI runs `flow.test.js`; regression tests pin M11/M12, M22, M26, M33, M52, M59, M60 and the reviewer's A2. |
| P3-1 | P3 | Reduced motion: a tab hide/show started a 60 fps loop redrawing the static picture. | `_start` returns in static mode. |
| P3-2 | P3 | The direction grid (`grid_half` under a full-resolution field) was never validated. | `dirGridsOk` per manifest: a failing grid disables the animation for that run (test). |
| P3-3 | P3 | Code 255 (never written by the circular coder) would be drawn as north. | Absent in `sampleDirRow` (test). |
| P3-4 | P3 | The outage counter was reset by direction successes and incremented by direction failures. | Field fetches only (test: three field failures with direction successes between them still trip the banner). |
| P3-5 | P3 | `setAnim(false)` before the first frame left the direction fetch alive. | Aborted directly (test). |
| P3-6 | P3 | Every playback step wiped the particle trails (`_rebuild` cleared the canvas). | No clear in particles mode when the view is unchanged (test). |
| P3-7 | P3 | The particle target was not re-derived on a resize. | Follows the view (test). |
| P3-8 | P3 | Misc: a direction fetched for an unavailable target; `setCoast` recovery did not rebuild the arrows; a phone crossing 7 / 7.5 blanked the arrows until the new resolution landed; opacity changes reached the particle contrast only at the next rebuild. | All fixed (the entry rule keeps the same step's direction across a resolution change; `setCoast` calls `onRedraw`; `setOpacity` refreshes the animator). |

Verified correct by reviewer A (evidence in `g10a-report.md`): in-flight ≤ 2 of both kinds across 750 fuzz runs × 40
operations and a 60-seek storm; a direction never under another step / run / field / resolution at every delivery
(held decodes, Update mid-flight, Off then On, field switches, resolution changes); caches by kind with keys carrying
run / res / field / step; nothing left after unmount or untick (listeners, timers, caches, `flow`, `dres`); animator
lifecycle (attach / detach idempotent, `_frame` after detach a no-op); zero typed-array allocations per animation
frame, `dt` clamped, `_adapt` within [150, target], dpr capped at 2; the samplers (0.25 presence rule, DIR_AGREE = two
equal nodes disagree beyond 120°, pole rows, dateline continuity, FROM→TOWARD, `screenVec`, the Mercator cap, `dirRes`
hysteresis); the session key `anim` and the page bootstrap unaffected.

Before G10 the author's own MAX-effort pass (27ebca3) had fixed: arrow anchors carried a WRAPPED longitude while Leaflet
keys its tiles by unwrapped coordinates, so the readout gate found no tile west of the dateline and dropped every arrow
there (the dateline is on screen at zoom 4 centred on Hawaii); a periodic hard clear of the trails (a blink every 4 s);
arrows not rebuilt when a coast chunk landed; wind-field buffers not reused.

## Reviewer B (9d8c6e6, live run 2026092518): 0 P0, 0 P1, 2 P2, 8 P3

Method: steps f000 / f024 / f048 / f120 / f240 of the first run with direction data decoded independently from the
NOAA records (Pillow JPEG2000 for DIRPW, eccodes for u/v); the client's own `decodePngGrey` + `dirAt` on the published
PNGs in Node against an independent circular-bilinear reference at ~600k points; a coordinate-recording canvas for the
chevrons; a 400-view lattice fuzz; the arrow gate on 12 real views × 4 steps; the forecast table at four Hawaii stations.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P2-1 | P2 | `ARROW_MIN` had no `tp` entry: the period layer drew chevrons over flat seas — 1.8–2.2 % of its arrows worldwide at zoom 5 sat over hs < 0.1 m, 80 % of them with Tp < 2 s (the model's no-wave floor along the ice margins); the README claimed the 0.1 m gate for both layers. | Fixed: a 3 s period floor on the period layer (test); README. |
| P2-2 | P2 | Between two regimes 60–120° apart the unit-vector mean drew a direction no node has (27–31 % of the node pairs with a period jump > 2 s per step; 1–9.5 % of the drawn Hawaii arrows on a 2×2 with > 90° spread, e.g. west of Niihau nodes 298/296/295/89 → 305°). | Fixed (author's call, within the plan's "skip where the neighbours disagree"): where the present nodes spread more than 60° the nearest node's direction is shown; within one regime the circular mean (tests: opposite nodes, a 140° edge, a 50° blend, the reviewer's 2×2). The old "beyond 120° → no arrow" rule is gone (reviewer A's M2 pinned it; superseded). |
| P3-1 | P3 | Half-resolution direction under the full-resolution field (desktop zoom 4–7.5): 0.40 % of 8,924 drawn coastal arrows differ > 45° from the full-resolution direction (0.07 % > 90°), mostly half nodes across land (Gulf of Taranto, Dover Strait); 1.2 % of coastal anchors lose their arrow at half resolution. | Owner awareness: full pdir from zoom 6 would cost ~12 MB more per loop only when zoomed (the §21 budget put pdir full from 7.5). Left as designed. |
| P3-2 | P3 | The job's mask-mismatch WARNING fired for one 0.01 m cell at f120 (−69.25 N 32.25 E, Tp 1.64 s), where no arrow can exist. | Deferred to a job-only batch: count mismatches at hs ≥ 0.1 m only. |
| P3-3 | P3 | "Dominant" = the spectral-peak direction: at 51004 the arrows show the SSW 9.5 s swell (the table's partition at that period) while the table's Swell 1 is the E trade sea. | README defines it (NOAA DIRPW, the partition carrying the peak period). |
| P3-4 | P3 | The particle contrast assumed a dark basemap (luminance 60): over deserts and ice sheets a white particle sat on a light composite at 6.5–26 m/s (contrast 1.2–1.7). | Fixed: every particle is a dark halo under a light core (still two strokes); the luminance rule is gone. |
| P3-5 | P3 | The trail-fade floor is a few /255 of alpha, not 1/255 as the comment said (invisible either way). | Comment fixed. |
| P3-6 | P3 | Particle age counted frames (1–2.5 s at 60 Hz, twice that at 30 Hz). | Age and life in milliseconds (test). |
| P3-7 | P3 | Build 664 s explained (+3.5 s per step: nearest fill via np.roll copies 0.8–1.3 s per field, pdir PNG optimize, the DIRPW download, three more sequential PUTs). | Deferred to a job-only batch (threaded uploads, a distance-transform fill). |
| P3-8 | P3 | Docs drift: the plan's "4-px screen field" (code 4/8 px) and "hard clear every few seconds" (removed at G10-A); README per P2-1. | README fixed; the plan's progress log records the deviations. |

Verified correct by reviewer B (numbers in `g10b-report.md`): the published PNGs equal `encode_frame()` of an independent
decode (0 differing cells in 45 comparisons: 5 fields × 2 resolutions × 5 steps); pdir codes 0..254, 255 never, 0 exactly
where the filled grid is NaN; wdir 1..254; `decodePngGrey` equals PIL byte for byte; `dirAt` at 20,000 nodes per frame
within 0.7086° of the truth (bound 0.7087°), random / dateline / pole / coast / calm sets with no null flips; lon ± 360
worst 4e-11°; the chevrons for nine FROM directions tip-ahead, on-track, symmetric at 32.09° (FROM 300 → screen (0.862,
0.507), south-east); 159,967 anchors over 400 views all at tile pixel 32 mod 64 with the unwrapped tile key, invariant to
pans, world copies and the dateline; the gate never draws without data or under 0.1 m on the wave-height layer; wdir vs
NCEP's own WDIR median 0.16°; the table's wind directions agree within 0–8° at 15 of 20 station-steps; DIRPW pairs with
the table partition carrying the peak period at every checkable step; the stats sidecar and manifest fields consistent;
the loop sizes match the README (20.3 / 32.6 / 11.6 / 23.0 MB).

## Reviewer C (test site, 9d8c6e6 / asset 2.8.0, live run 2026092518): 0 P0, 1 P1, 3 P2, 4 P3

Method: the in-app browser on wave-app-clean.onrender.com at 1280×800 and 375×812 (Oahu z6/z8/z10, the North Pacific
z3/z4 with the dateline on screen, Norway z8, the equator, z1, z11, fractional zooms), the controller's own state
sampled every 50 ms through playback loops, network sums, `performance.memory`, long-task observers. The reviewer's
first pass ran on a stale build from the browser's immutable cache (the first 2.8.0) and was redone on the served one.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | Arrows vanished after any Leaflet view reset at the same zoom and stayed gone while paused: `setView` without animation, a pan of a screen or more, and the site's own `enforceSingleWorld` on a window resize (the map jumps back with 0 arrows, 181 expected). Cause: `viewprereset` removes every tile, `moveend` rebuilds the arrows against an empty layer, `viewreset` recreates the tiles and nothing rebuilt. | Fixed: the animator rebuilds on `viewreset` too (test). |
| P2-1 | P2 | The arrows blinked off at every playback step (no direction in 42–48 % of 50-ms samples at 4×, 10 % at 1×): the picture was shown before its direction landed. | Fixed: with Animation on a step waits for its direction frame beside its field frame, so both change together; a missing, failed or aborted direction never holds the step (`_dirReady` settles on the fetch's own abort — found in the fix round: a decode that cannot be cancelled must not hold it). Tests: the M26/P2-1 staging, the dropped-direction staging. |
| P2-2 | P2 | On a fresh load the animator used Leaflet's stale map size (414×458 for a 414×134 container: the site sets the map height by script after Leaflet measured): a 458-px canvas and only 28 % of the particles inside the visible map until the first resize. | Fixed: the animator sizes itself from the container, as the panel already does (test). |
| P2-3 | P2 | Four builds went out under the immutable `?v=2.8.0` URL on the test site; a browser that opened it before 22:33 UTC still ran the first one. | Asset **2.8.1** for the production merge; the version is bumped on every push from now on. |
| P3-1 | P3 | Phones: the sheet is 38.8 % of the map and clear of the zoom stack, but the Opacity/Contours/Animation row sits below the fold of the 66-px details box (pre-existing G8 B-P3-4, deferred by the owner), and a viewport change that stays compact left the sheet un-capped (68 % of a 148-px map) until a re-render. | The un-capped case fixed (a compact→compact resize re-renders the sheet); the fold stays deferred with G8's item. |
| P3-2 | P3 | The trail-fade floor is up to 4/255 (invisible). | Comment already corrected (G10-B). |
| P3-3 | P3 | Frames are fetched ~1.8× per 4× loop (212 field + 211 direction fetches for ~115 steps), with Animation off as well: the ring trimming at 4× (pre-existing). | Owner awareness; a later improvement (a longer ring at 4×). |
| P3-4 | P3 | Long tasks at 4× roughly quadruple with Animation on (52–60 of 50–64 ms per 26 s vs 12 off); the animation's own work per step is small (setData 0.1–0.4 ms, a direction decode 3–9 ms) beside the 12–50 ms tile redraw. | Owner awareness (the profile changes with P2-1: the picture and the arrows are now set in one task). |

Verified correct by reviewer C (numbers in `g10c-report.md`): a new tab starts Off (module not loaded, 0 requests), the
checkbox unchecked and enabled; tick → arrows in 134 ms; the Animation setting, field and time restored across a
forecast-point reload (paused wave height → the same frame; playing wind → wind + Animation + playing at the new station);
the arrow gate clean (no arrow over land, none under 0.1 m, none without a direction) at Oahu z6/z8/z10, the North Pacific
z3/z4 with the dateline on both sides (70/70, 79/79, 21/21 anchors east of 180°), Norway z8, the equator, z1, z11, 8.5, 7.6
and the phone; directions plausible (51201 at 21Z: pdir 75.5° vs the table's swell 1 at 55°; wind 62° / 20 mph vs 64° ENE /
24 mph); the chevrons move TOWARD the direction of travel (screenshots 0.7 s apart); wind particles flow over land, trails
fade with no visible residue at 60 s; the direction's step equals the layer's step in 1,107 of 1,107 samples; in-flight ≤ 2
in every loop; frame cost 0.02–0.12 ms (arrows) and 0.03–0.07 ms (particles) against the 4 / 8 ms budgets; bytes per
81-frame loop 19.5 MB desktop default (hs full + pdir half), 31.3 MB at zoom ≥ 7.5, 11.1 MB phone, 21.8 MB wind + wdir;
heap live set of the animation ≤ ~5 MB; the zoom animation hides the canvas and rebuilds (235→236 at z9, 149→323 at 7.6 with
the full-resolution direction, no blank across 7 / 7.5); a hidden pane stops the loop and it restarts on show; Off tears
everything down (panes empty, `flow` / layer null, caches 0, 0 bucket requests in 5 s); untick stops the direction fetches
while the field keeps playing; a drag through the pane moved the map exactly 80 px; a live-buoy click opened its panel with
playback continuing; readout units follow the unit switch; Home preset, zoom clamp, contours + animation together fine;
console clean apart from one Render cold-start 503. Not verifiable in the pane: pinch, a real tab switch, a run without
direction data (simulated).

## Owner feedback after G10, and G11 (asset 2.9.x)

On the test site the owner asked for particle animation on every overlay (no arrows), and pointed at jerky, straight
particle streaks around a low-pressure centre on the wind layer. Cause of the streaks: the nearest-cell rule of G10-B
P2-2 (right at a swell-regime edge) snapped the direction wherever neighbouring cells differ by more than 60°, which is
everywhere around a cyclone; plus per-cell velocity reads and an Euler step. Asset **2.9.0–2.9.2**: the chevrons are
gone from every layer (under reduced motion nothing animates and no direction frame is fetched; the checkbox is
disabled and says so); the flow is built as VECTORS on the model nodes (`vectorNodes`: the field value under the node
times its FROM direction, once per step) and interpolated between nodes as vectors on the screen lattice (`flowField`,
per view; the land gate through the tile masks for the wave fields), so a cyclone turns smoothly and slows to nothing at
its eye; the particles read the lattice bilinearly and advance with a midpoint step; swell particles at 8 + 3 px/s per
metre (wave height) or 1.5 px/s per second (period), 2–4.5 s lives; `arrowAnchors`, `screenVec`, `dirAt`,
`sampleDirRow` and the ARROW constants were removed.

### G11 (one fresh reviewer at MAX, asset 2.9.1 @ edfd0ec, live run 2026092518): 0 P0, 1 P1, 3 P2, 2 P3 — fixed @ 73daf40 (2.9.2)

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | Reduced motion: the checkbox was neither disabled nor annotated (the README claimed it), and every step still fetched its direction frame and waited for it, for an animation that never draws. | Fixed: `_wantDir` is false under reduced motion (no flow, no fetches); the checkbox is disabled with "Off under your reduced-motion setting" (test). |
| P2-1 | P2 | The compositing fade (`destination-in` at 0.9 per frame) never reaches zero in 8 bits: after minutes on a static view 28 % of the canvas kept alpha 5/255 — a permanent 2 % veil with ghost paths. | Fixed: no compositing fade; each particle keeps a short history (ten positions, one every 66 ms) and is drawn fresh over a cleared canvas as a dim tail plus a bright head (four strokes a frame); nothing accumulates (test). |
| P2-2 | P2 | Swell particles ran up to one lattice cell onto the land mask (0.21 % of segments ended over the coast mask at Hawaii z6.6): the respawn check read only the cell's top-left node. | Fixed: a particle respawns when any of its four surrounding lattice nodes has no flow (test). |
| P2-3 | P2 | 18 of 39 single mutants survived: node vectors never recomputed for a new step, nearest instead of bilinear lattice reads, absent nodes blended, the 0.25 rule, the column wrap, fractional-zoom tile keys, node buffer reuse across a resolution change, ageing, the fade exponent. | Tests added for each (113 Node). |
| P3-1 | P3 | README wording "the readout's own rule" overstates the gate (the land part only; data presence is approximated on the direction grid). | Wording left; noted. |
| P3-2 | P3 | Vector averaging across a 180° reversal gives a stationary line at the eye (by design; nothing visible on the live run). | Accepted. |

Verified correct by the reviewer: `vectorNodes` on live frames against an independent decode — 0 mismatches over
1.24 M nodes for all four field/direction pairings; FROM→TOWARD and the speeds match the readout (the storm's 32.9 m/s
cell); the land-gate tile keys match Leaflet's `_tiles` at fractional zooms 7.3 / 6.6 and across the dateline; costs:
node vectors 5–17 ms per step, the lattice 0.2–2 ms per view, the render 0.28 ms at 526 particles (1.0 ms at 3,000) on
desktop and 0.04 / 0.28 ms on the phone preset; heap 14.2 → 17.4 MB; in-flight ≤ 2; the layer's step equals the flow's
step in every sample over 8 steps at 4×; untick removes the canvas, all listeners, the hook, the cache and the fetches;
the step delay with Animation 372 vs 312 ms.

## Outcome

G10: **0 P0, 2 P1, 8 P2, 20 P3** across the three reviewers; every P0/P1/P2 fixed (b4e20fc, 718f41d, b984c8d) and the
reviewers' own reproductions pass against the fix (reviewer A's suite 40/43: the three remaining assertions pin superseded
behaviour — five map listeners, the 120° rule, a first landing without the direction wait). Deferred with the owner's
awareness: half-resolution direction under the full-resolution field, the phone controls below the fold, the 4× ring
refetches, the long tasks at 4×; job-side: the mismatch-warning threshold and the build time. Shipped as asset **2.8.1**.
