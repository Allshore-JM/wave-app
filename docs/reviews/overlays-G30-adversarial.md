# G30 — adversarial review of the overlay model dropdown (GFS-Wave | ECMWF), plan section 43 step 5

Target: `feat/ecmwf-overlays` @ 2374454 (client, asset 2.17.0) on b2a87e2; test site `test` @ 6cad697 with
`MODEL_OVERLAY_MODELS=gfswave,ecmwf`. Two fresh-context reviewers, 2026-10-15, effort MAX:
- **A** (code + Node, model Fable 5.1): baseline 659 Node / 1128 pytest green; a two-model fuzz on three decode paths
  (main, canvas, worker) with the SAME run names on both models in half the seeds and held pointer / manifest / decode
  answers released in random orders: 22,730 valid runs, 1.48 M ops, 40,590 model switches, 28,536 switch landings; 17
  targeted cases; 6 panel cases; 72 mutants (57 killed, 10 equivalent, 5 test gaps). Scratch `g30/a`.
- **B** (test site + use, session model Opus 5.5): headless Edge with the GPU, trusted keys / clicks / touch, desktop,
  tablet, phone and landscape; 40+ switches in every state; failure paths (pointer / manifest / frame errors, hangs,
  stale and newer-run banners with real runs, incl. ECMWF's 12Z published during the review); credit terms; interplay;
  a11y; timed cells back to back after A finished. Scratch `g30/b`.

## Result: 0 P0, 1 P1, 3 P2, 12 P3 (distinct; A 0/0/2/7, B 0/1/2/8, four overlaps)

Held up (both reviewers): no field or direction frame of one model was ever fetched, cached (by key or by value),
landed, drawn or animated under the other — also with both runs named 2026101512 after ECMWF's 12Z publish (only the
model in the key separates them; B compared 12/12 decoded frames to the bucket PNGs); every key carries model + run; the
first landing after a switch is `nearestIndex` of the requested time (paused, playing at every speed, worker decode,
mid-drag, pending seek, reload, error panel, Retry, Update); the carry is spent only by a landing; late pointer / run-check
/ Update answers of the old model are ignored; per-model base, Next Update, stale line, credit and run tail are right; the
choice survives Off/On and reload, a new tab starts on GFS; one credit at a time in Leaflet's counting control; nothing
left after Off; the paint gate never stalls after a switch; performance parity on warm runs (0 unpainted frames,
fetch/frame 1.00-1.02, in flight within the cap, equal heap; a no-network draw benchmark 125 vs 126 ms hs, 107 vs 105 ms
wind; switch key -> paint p50 320 vs 317 ms).

Claims: C1 PARTLY (reduced motion), C2-C5 CONFIRMED, C6 PARTLY (one-line heads, loading panel), C7 CONFIRMED, C8
CONFIRMED, C9 CONFIRMED on warm runs.

## Findings

### P1
- **G30-1 (B F-1, A A-2, author's K-7) — one-line heads and phone landscape lose the time (a regression from
  production).** The coarse-pointer select is 32 px where the head line was 28 px (fine pointers: 24 vs 18 px); the
  taller head folds the sheet one size earlier, and every one-line head (the `compact && cap - h - 8 < 40` early return
  AND the `!opens` size fold) keeps the dropdown as its title instead of the summary line. At 812x375 / 667x375 (small
  iPhones sideways) the sheet loses the ribbon and its time that production shows; at 844x340, 800x360, 740x300,
  667x260 and desktops <= 320 px tall the one line is [play][dropdown] with no time. Author reproduced 812x375: test
  [play][select], production toggle + name + ribbon "Thu, Oct 15 · 12 PM HST". A control with one model offered on the
  same page restores production's layout. Fix: keep the head at its old height; every one-line head shows the summary
  line (the model is changed from the open panel); panel tests for both one-line branches with two models.

### P2
- **G30-2 (A A-1, B F-5) — under reduced motion a model switch drops the play state** (mount's resume gate
  `reducedMotion()` was written for a reload's restore); Update and a field switch keep playing. Fix: apply the gate to
  restores only.
- **G30-3 (B F-2, A A-6) — the loading panel has no dropdown; a hanging pointer / manifest request traps the tab on the
  new model.** The model is saved before anything lands and the pointer / manifest fetches have no watchdog, so a field
  switch, Off/On and a reload all retry the same model; the everyday form is a slow switch with no way back until it
  lands (10.4 s measured on a slow pointer). Fix: the head (toggle + dropdown) in the loading panel when more than one
  model is offered; a watchdog on the pointer and manifest fetches (-> the error panel, which has the dropdown); save the
  model when its first frame lands (also closes G30-5).
- **G30-4 (B F-3, author's K-9) — the ECMWF credit falls short of ECMWF's Terms of Use and keeps the CC BY 4.0
  modification notice in a hover-only title.** ECMWF's terms (https://apps.ecmwf.int/datasets/licences/general/) ask
  that attribution be "displayed prominently" and give the wording for services (based on data and products of ECMWF;
  Source www.ecmwf.int; the CC BY 4.0 statement; the liability disclaimer; an indication of modification). On the page the
  whole credit sits behind the (i) on every device; the modification statement and "does not endorse" live only in the
  ECMWF link's `title` (invisible on touch); no notice file exists beside the frames. CC BY 4.0 §3(a)(2) allows a link to
  a resource carrying the required information. Owner decision 7 stands; this is a compliance risk -> owner's call.

### P3
- **G30-5 (A A-3, B K-8)** a failed switch leaves the tab's save as {new model, the old model's time, playing: false}
  (`pause()` persists after `this.model = key`): a reload restores the new model paused, at "now" when 6-hourly frames
  are > 90 min away.
- **G30-6 (A A-4)** `update()` ignores a pending carry / restore (UI-unreachable: no banner while loading).
- **G30-7 (A A-5)** `_loadManifest`'s manifest continuation and `update()` adopt without re-checking the aborted signal
  (safe only because browsers reject an aborted body read): add the check.
- **G30-8 (A A-7, B F-4)** a model switch on wind shows the satellite imagery for the whole load (relief -> imagery ->
  relief; ~250 ms warm, seconds cold); Retry and Update keep the relief.
- **G30-9 (A A-8)** `MODEL_DEFS[key]` truthiness accepts inherited keys (`constructor`): use hasOwnProperty.
- **G30-10 (A A-9)** test gaps: `_dropRun` keeping `newerRun` (another model's banner), `unavailable`, `wasPlaying`,
  `transientFails`, and `setModel` ignoring `wasPlaying` survive the repo suites.
- **G30-11 (B F-6)** the aria-label "Forecast model" reads as the forecast table's model: "Overlay model".
- **G30-12 (B F-7, risk)** the 11-12 px select on phones may make iOS Safari zoom on focus (the site's own settings
  selects use 16 px for this reason).
- **G30-13 (B F-8, pre-existing)** the error panel's Retry is 41x22 px on touch (< 24 px).
- **G30-14 (B F-9)** the folded and loading panels never name the model -> owner's call.
- **G30-15 (B F-10)** a round trip GFS -> ECMWF -> GFS re-snaps at each switch and drifts off the time it left (up to
  30 h past ECMWF's end) -> owner's call (keep the originally requested time across consecutive switches).
- **G30-16 (B F-11)** speeds are frames per second, so the same animal plays ECMWF ~3x faster through forecast time
  (6-hourly frames: 6x) -> owner's call.

## Outside the client (for the owner)
- ECMWF's 12Z build on 2026-10-15 waited out the throttle ladder on the AWS mirror and was stopped by the job's 60-min
  limit (no failure record): fixed by throttling hotfix 2 (production @ 1968990, 20:49 UTC): a throttled mirror read moves
  to data.ecmwf.int at once; a 45-min build budget exits quietly. The run published at 21:09:47 UTC (verified vs eccodes).
- Neither frames job warms the edge after publishing: a new run is cold per Origin for its first viewers (MISS 318-577 ms
  per frame vs 20-35 ms HIT; a switch onto the just-published ECMWF 12Z took 1061 ms key -> landing). Optional follow-up.

## Owner decisions (2026-10-15)
- G30-4 credit: **a notice file only** — publish the full ECMWF notice (copyright, based on data and products of ECMWF,
  source www.ecmwf.int, the CC BY 4.0 statement and link, ECMWF's disclaimer, our modifications, no endorsement) and link
  the credit's "ECMWF" to it; nothing new in the panel.
- G30-14: with more than one model offered the one-line / folded summary names the model ("Thu, Oct. 15, 11:00 AM HST ·
  ECMWF").
- G30-15: a switch returns to the time the viewer chose: the requested time is kept across consecutive switches until the
  viewer moves the time.
- G30-16: speeds stay frames per second (every frame shown; ECMWF covers its 15 days faster).

## Fix round scope (asset 2.17.1; Opus 5.5 HIGH)
1. G30-1: the head keeps its old height (coarse select 28 px, fine no extra height); both one-line branches show the
   summary line (with the model per G30-14), never the dropdown; panel tests for both branches with two models and the
   812x375 / 1280x320 cases.
2. G30-14: summary line + " · GFS" / " · ECMWF" when more than one model is offered.
3. G30-15: the requested time (`_switchT`) is kept across consecutive switches; any landing that is not a switch's
   landing (seek, step, play, Update, field-switch landing after a user move) clears it.
4. G30-2: the reduced-motion gate applies to reload restores only.
5. G30-3: the head (toggle + dropdown) in the loading panel when more than one model is offered; a watchdog on the
   pointer and manifest fetches (-> the error panel); the model is saved when its first frame lands (closes G30-5).
6. G30-4: the notice (owner decision) and the credit link to it.
7. P3s: G30-6 (`update()` honours a pending carry), G30-7 (aborted-signal checks before both `_adopt`s), G30-8 (the wind
   relief kept through a switch), G30-9 (hasOwnProperty), G30-10 (pins for the five survivors), G30-11 ("Overlay model"),
   G30-13 (Retry / Update >= 28 px on touch). G30-12 (iOS zoom on an 11-12 px select; the field select has the same,
   pre-existing): accepted pending the owner's iPad check.
8. Merge production (1968990, throttling hotfix 2) into the branch (no rebase).
Then: the test site (MAX), a short fresh re-check (MAX), production (tag `prod-pre-ecmwf`).

## Fix round (asset 2.17.1, 2026-10-15, Opus 5.5 HIGH)
Every item of the scope is built:
- G30-1: the dropdown keeps the head line's height (coarse 28 px, fine no padding); both one-line branches (the short-sheet
  early return and the `!opens` size fold) swap the dropdown for the summary span (`oneLine()` in render).
- G30-14: the summary line ends with " · GFS" / " · ECMWF" when more than one model is offered (`MODEL_DEFS[*].short`), before
  a pending seek's " · loading…".
- G30-15: `_switchT` keeps the chosen time across consecutive switches; seek, step, play, Update and Off end it.
- G30-2: the reduced-motion gate applies to reload restores only (`!carry && reducedMotion()`).
- G30-3: the loading panel carries the dropdown (the focus stays on it); `_fetchJson` gives the pointer and the manifest a
  15-s watchdog (`LOAD_STALL_MS`) that ends in the error panel ("no answer from the frame host"), cleared by an abort; the
  model is saved by `_persist` when its first frame lands, and `setModel` pauses (saving the OLD model) before switching,
  keeping the play state in the save (closes G30-5).
- G30-4: `/notice/ecmwf` (app.py `ECMWF_NOTICE`, plain text, this year's copyright line, public 1 day) and the credit's
  "ECMWF" links it.
- G30-6: `update()` lands at a pending carry's time. G30-7: the aborted check before both `_adopt`s. G30-8: `_dropRun` and
  `mount` leave the look alone while the field stays wind. G30-9: `hasModel` (own keys). G30-11: aria-label "Overlay model".
  G30-13: Retry / Update >= 28 px on touch. G30-10: pins for the reviewers' survivors (banner, unavailable list, outage
  count, a hidden tab's playback). G30-12 accepted pending the owner's iPad check.
Tests: overlay Node 279 (+13), UI 393, pytest 1,133 (the notice); mutants of the fix 21: 20 killed (F17, the aborted check
before `_adopt` in `_loadManifest`, is a second guard: the watchdog fetch already rejects on abort). Production merged in
(944f055: throttling hotfix 2).

## Test-site verification of the fix round (2026-10-15, Opus 5.5 MAX)
`test` @ 3ae688c (2.17.1, a cherry-pick of 6f9ff0e; the G30 record taken from the feature) then 6eeb842 (2.17.2); the site
files equal the feature's apart from the test-only logo; overlay Node 278/278 on the test branch; served assets = the commit,
the previous version's URL 404. Headless Edge with the GPU, trusted keys / clicks / touch; scratch `ecmwf/v4/hl`
(`v3_sizes.js`, `v4_g30fix.js`, `v5_probe.js`, the step-4 `v1_func.js` / `v2b_banner.js` / `w54e.js`).
- **Found and fixed (2.17.2 @ 6eeb842):** 2.17.1's mouse-pointer dropdown was 20 px where production's head line is 18.4 px, so
  a 1280x320 map still folded the sheet one size early (no ribbon, no time; production keeps them). Probed on the page, then
  `height: 18px; box-sizing: border-box` (touch keeps min-height 28 px); CSS pin in panel.test.js.
- **G30-1 / G30-14, production 2.16.6 vs test 2.17.2 at 15 sizes** (812x375, 667x375, 844x340, 800x360, 740x300, 667x260,
  568x220, 844x390, 375x812 touch dpr 2; 1280x320/300/260/400/650 and 1920x1080 mouse): 15 of 15 identical — the ribbon
  where production has it, the same head heights (28 px touch, 18.4 px mouse) and panel / sheet heights to 0.1 px; every
  one-line head is [play]["Thu, Oct. 15, 01:00 PM HST · GFS"], "· ECMWF" after a switch; no sideways scroll.
- **G30-3 / G30-5:** ECMWF's pointer held at the network: 30 samples of the loading panel, every one with the dropdown
  (focused); the error panel "no answer from the frame host" at 15.5 s with the dropdown focused; the tab's save still GFS; a
  reload restores GFS at the same time. Escape: 3 s into a held load the loading panel's dropdown (trusted ArrowUp) goes back
  to GFS at the same time.
- **G30-15:** GFS 21Z (between ECMWF's 6-hourly frames) -> ECMWF 18Z -> GFS 21Z again; a trusted ribbon step then a switch:
  from the stepped time (00Z on both).
- **G30-2:** prefers-reduced-motion emulated: a playing switch keeps playing, both ways.
- **G30-8:** GFS wind on the relief, ECMWF frames slowed 2.5 s: 88 samples (55 with nothing drawn), never the imagery; the
  relief after the landing.
- **G30-4 / G30-11 / G30-13:** the credit "Overlay: ECMWF CC BY 4.0 · GSHHG" links `/notice/ecmwf` (new tab); the page 200
  text/plain, public 1 day, with the copyright (this year), source, licence and link, the changes, ECMWF's disclaimer and no
  endorsement; aria-label "Overlay model"; on a phone the error panel's Retry is 41x28 px, the dropdown 172x28 px.
- **Regressions (the step-4 scripts on 2.17.2):** the paused switch (exact time, focus kept, no GFS frame after), decoded
  frames = the bucket's PNGs (ECMWF and GFS, field and direction), the credit and run lines, playing switches both ways (no
  frame of the old model started or drawn after the switch), the pending-scrub switch (requested 17Z -> ECMWF 18Z, not near
  the drawn frame), Off/On, reload, the error path, the phone sheet, a new tab on GFS, the newer-run banner and Update with
  real runs (ECMWF 00Z served first: no stale line at 12 h; "A newer run (20261015 12Z)"; a trusted Update), a switch while
  the banner shows: all pass; console clean apart from the injected connection failure.
- **Timed (GPU, both runs 12Z warm for the test Origin):** ECMWF shark 1080p 8.0 fps 125/135 ms, GFS 125/136, ECMWF phone
  124/135; 0 skipped, 0 unpainted, fetch/frame 1.02, in flight within the cap; heap live +31.5 / +31.8 / +12.6 MB.
- Cleanup: the test origin's storage emptied after every script; Edge stopped (profile removed, no process left).

## Re-check of the fix round (2026-10-15/16, one fresh reviewer, Opus 5.5 MAX): 0 P0, 0 P1, 0 P2, 6 P3
Scope: the client diff 2374454..6eeb842 and the test site (2.17.2). Evidence: G30-A's two-model fuzz adapted to the new rules
plus five new invariants (chosen time across switches, the save only after a landing, no watchdog timer outliving its
request, a way to the other model in every panel state, reduced motion): 9,400 seeds, 26,547 switches, no frame / cache /
credit / banner / save of one model under the other, never stuck, a reload never trapped; 55 own mutants (40 killed, 9
equivalent incl. F17, 6 behaviour-changing survivors = R-4/R-5 below); on the site 302 one-pixel height steps against
production (mouse and touch) with identical folds and head heights, the watchdog at 14 s (lands) and 16 s (error at 15.3 s,
the late answer never lands; the manifest likewise), the loading panel's dropdown by trusted click and keys (a 9-key burst),
Tab order, the status node, the notice by a trusted click, reduced motion, timed parity (two back-to-back pairs, 8.0 fps,
p50 125 ms, 0 unpainted). Claims R1-R10 CONFIRMED (R1 with R-1/R-3), R11 PARTLY (the author's 21 mutants not re-run).
- **R-1 (P3, accessibility)** a browser minimum font size of 14 px or more clips the 18-px dropdown's label (production's
  span grows). Fix proposed and measured in the page: `height: auto; margin: -1px 0` (the select out of the head's height):
  plain dpr 1 head 18.39 = production, 16-px minimum font label whole with the head 23.19 = production, real 150 % head
  17.73 = production.
- **R-2 (P3)** a failed model switch on wind leaves the bare relief (and the blend) with nothing drawn under the error panel;
  from wave height the same failure shows the imagery. Fix: `_fail` syncs the look when nothing is drawn.
- **R-3 (P3)** at a real 150 % display scale borders snap to 0.667 px: the test head is 18.00 px vs production's 17.73 (no fold
  changed in 181 heights); fixed by R-1's rule.
- **R-4 (P3, tests)** `_switchT` not cleared by Off, and `_switchT = shown` instead of the chosen time, survive the suites.
- **R-5 (P3, tests)** nothing pins that the watchdog or an abort cancels the network request itself (the signal given to fetch).
- **R-6 (P3, docs)** the watchdog is a 15-s deadline per pointer / manifest request (headers and body), on every such load
  (GFS's own first load, a field switch's re-read, Update), not "silence on a switch".
- Pre-existing, not counted (also on 2374454 and production's Update path; G30-A saw the class): an Update racing a playback
  landing, or an Update whose frames never answer, leaves the old run's picture of the same model with `frameIndex` null
  under a ready or error panel. A switch is immune (`_dropRun`).

## Fix round 2 (asset 2.17.3, 2026-10-16, Opus 5.5 HIGH; the owner also asked for the pre-existing Update race)
- **R-1 / R-3** the dropdown's fixed height is gone: `margin: -1px 0` keeps it out of the head line's height and the browser
  sizes it, so a larger minimum font grows it unclipped (as production's span does) and borders that snap at 150 % add
  nothing. The touch rule (`min-height: 28px`) is unchanged.
- **R-2** `_fail` syncs the look whenever nothing is drawn: a failed switch on wind gives the imagery back (the basemap
  follows what is drawn, as everywhere else).
- **Update race (pre-existing)** two parts. (a) `update()` drops the pending target at once, so a step or playback landing of
  the run being replaced (its frame already decoded) can no longer pass `_goto`'s checks and put the old run's ready panel
  over Update's loading one. (b) A load that fails with no frame of its run in hand clears the predecessor's picture, its
  particles, the readout and the overlay credit: never a picture without its frame and run under the error panel.
- **R-4 / R-5 pins**: the chosen time is what a run of switches keeps (never a snapped frame's) and Off ends it; the
  watchdog and a switch away abort the signal fetch itself was given.
- **R-6** README: the 15-s deadline covers every pointer / manifest request (switch, first load, a field switch's re-read,
  Update), headers and body; a failed load leaves nothing of an earlier run; Update drops the pending step.
- Tests: the three behaviour fixes each fail on 2.17.2 and pass on 2.17.3; the CSS pin asserts no fixed height. Overlay
  Node 284, UI 393, pytest 1,133. Mutants: 11 of 11 killed (no clear / no look sync / clear with a frame on the map, which also pinned the outage's picture / Update keeping its target / Off keeping _switchT / _switchT = the shown frame / fetch given the outer signal / abort or watchdog not cancelling the request / the fixed height back).
- Fuzz (the re-check's two-model fuzz against the new source): 1,350 runs, 3,626 switches on the main, canvas and worker decode paths (interactive and playback), 0 failures. The three seeds that failed "I1 drawn" on
  2.17.2 (10459 main; 10624 main and canvas; 31762 main, interactive) pass.

## Test-site check of fix round 2 (2026-10-16, Opus 5.5 MAX): two more assets, 2.17.4 and 2.17.5
Headless Edge with trusted input (scratch `g30/check2`), production (2.16.6) beside the test site. Served bytes = the repo
for 2.17.3 (test @ fc64ec8), 2.17.4 (@ 369f0a0) and 2.17.5 (@ bff2047, live 02:40 UTC); each earlier version answers 404.
- **The Update race on the live sites** (GFS, shark playback, the particle-direction frames slowed 600 ms, the newer run's
  manifest held, a trusted click on Update; the "newer run" = the live run under another name): production 11 of 12 clicks
  landed a frame of the run being replaced over Update's loading panel; the test site 0 of 12 (the panel stayed "Loading").
  Both landed the new run at exactly the requested time 12 of 12 and resumed playback.
- **An Update whose frames never come** (the new run's frames answer 404), paused and playing: production keeps the old run's
  picture under the error panel (98,129 painted samples, frameIndex null); the test site clears it (0).
- **R-2**: GFS wind on the relief, the ECMWF pointer refused: the error panel over the imagery (relief removed, no blend);
  back to GFS: the relief alone once the designed fade has run.
- **Sizes and folds** (2.17.3 and again 2.17.5): the 15 window sizes identical to production; one-pixel height scans 1280 x
  300-480 mouse (181), 812 x 300-420 touch (121), emulated 1.5x (16): 0 layout or height differences, identical fold points.
- **FOUND (P3, both fixed during the check)**: R-1's rule (-1px, measured by the re-check at 12 and 16 px only) let the
  dropdown set the head's height at other minimum font sizes: the toggle sets it in production (1.2em + 2px padding + borders)
  while the dropdown follows its font's natural line (about 1.34em for Segoe UI). Measured on 2.17.3: equal at 12 / 13 / 15 /
  16 px, +0.2 px at 14, +0.41 at 18, +1 at 20, +1.2 at 24 (desktop and phone). 2.17.4 (@ 74680d0): beside the toggle an em
  margin (`.ov-head .ov-toggle + select.ov-model-sel`), -0.15em, equal at every size by injection and on the site. But browsers
  resolve that em against the 12-px font before the minimum font size enlarges the text (the margin measured -1.8 px at every
  size), so only 0.4 px to spare at Chrome's 24-px maximum and short of Firefox's 72-px range: 2.17.5 (@ b340fcc) -0.5em (6 px).
  The dropdown's box and label are unchanged (it stays centred on the line); the loading panel's row (no toggle) keeps -1px.
- **2.17.5 against production**: minimum font 12 / 13 / 14 / 15 / 16 / 18 / 20 / 24 px head heights equal on the desktop
  (18.39 / 19.59 / 20.8 / 22 / 23.19 / 25.59 / 28 / 32.8) and on the phone (14 / 18 / 24 px), panel heights equal; labels
  whole (the dropdown 20-36 px tall where its line and borders need 16.4-30.8 px). A real 150 % scale (forced device scale factor): head 17.73 =
  production (2.17.2: 18.00), and a 181-height scan at that scale: 0 differences.
- **Timed** (2.17.3; 2.17.4 / 2.17.5 change one CSS margin and no script): 1080p shark, production GFS 125/135 and 125/135 ms,
  test GFS 125/137, test ECMWF 125/140, ECMWF phone 126/133: 8 frames/s everywhere, 0 skipped, 0 unpainted, fetch/frame 1.02,
  in flight at the cap; heap live test +31.8 MB against production +32.5 / +31.8. (Production's lines read "FAIL: model
  undefined": 2.16.6 has one model and no `model` field, which the script expects; its timings meet the line.)
- **Regressions**: the fix round's own checks again on 2.17.3 (hold 15.5 s with the dropdown and the focus kept, escape while
  loading, round trip, reduced motion, wind relief through a slow switch, the notice, 28-px Retry) and the loading / error
  panels on 2.17.5: all pass. The Update banner with real ECMWF runs: Update by a trusted click lands 2026101512 at the same
  valid time, the banner goes with a switch, playback through a switch only ECMWF frames; the previous run (2026101500) now
  shows the 15-h stale line (published 15.3 h before), as designed.
- Cleanup: both origins' storage emptied after every script; every harness browser closed, profiles removed.

