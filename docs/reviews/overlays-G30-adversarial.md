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
