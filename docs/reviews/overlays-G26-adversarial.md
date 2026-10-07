# G26 — adversarial review of the model-overlay panel redesign (plan section 37)

Reviewed: `feat/overlay-panel` @ 6db621b (overlay asset 2.14.4; test site `test` @ ca45a07), off production
`Live-Buoy-Update` @ 8f73527 (asset 2.13.0, the old panel). Two fresh-context reviewers (Fable 5.1, MAX), 2026-10-07,
briefed with the owner's requirements and seven decisions, the author's claims and the known items K-1..K-10
(scratch `g26/brief.md`); reports in scratch `g26/a/g26-a-report.md` (code and geometry, no browser; own detached
worktrees, an independent Intl scan, fuzzers, 300 random scrub sequences, mutants) and `g26/b/g26-b-report.md` (the
test site, desktop and phone: the in-app pane with real input, then a headless Edge over the DevTools protocol with
trusted mouse / touch / key events; 28 screenshots). The author verified every finding below (the reproductions re-run,
the code read) before deciding its outcome.

## Result
**0 P0, 0 P1, 4 P2, 9 P3** (A: 3 P2 + 6 P3; B: 1 P2 + 2 P3, one of them = K-1). Nothing wrong with the data under a
label: the picture is always the labelled frame (A's fuzz; B's out-of-order and late-frame drills). The ribbon maths
agrees with an independent computation in 198 layouts over 16 zones except in zones whose clock change falls at local
midnight (P2-1). The defects are two touch-gesture races found by tests (not yet seen on a device), one geometry gap,
and a pre-existing desktop clamp.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| A-P2-1 | P2 | Zones whose DST change falls at local midnight: the day without a 00:00 gets no ribbon label (America/Santiago, America/Havana, Atlantic/Azores, Asia/Beirut, Africa/Cairo spring days), the day with 00:00 twice gets two labels 1 h apart (Havana, the Azores in autumn). 83 stations in the Azores zone, 10 in Havana, 3 in Santiago; two days a year each. The time under the pointer stays right. Author re-ran: 9 of 10 cases as reported (`g26/author/p2_1_check.js`). | FIX: push a day label when the local DATE changes between walk steps (the first pinned from the first step); tests for the Azores and Havana runs |
| A-P2-2 | P2 | A tap whose finger rolls 1-3 px just before lift-off: the release seeks the tapped frame, then the still-pending rAF scrub fires with the pre-release frame and undoes it (`seeks [6, 3]`). Browser-timing dependent (Chrome aligns moves with rAF; WebKit/Firefox may not). Author re-ran A's TAP RACE test: fails as described. | FIX: `release()` drops the queued scrub (`pendingIdx = null`, cancel the rAF) before its own `_scrubTo` |
| A-P2-3 | P2 | `pointercancel` is handled as a lift-off: under `touch-action: pan-y` a mostly vertical swipe that starts on the ribbon is cancelled by the browser with a sideways travel under 4 px, so the tap rule picks the frame under the finger (a 9 h jump the viewer did not ask for). Code-level repro (A's POINTERCANCEL test fails as described); whether Chrome / iOS fire it here was not verified on a device. | FIX: on `pointercancel` end the drag without the tap rule (snap to the nearest frame of the current offset); `touch-action: none` on the ribbon (nothing scrolls under it: the page does not scroll and the sheet's details box is below it), so the cancel path is not taken in the first place |
| B-1 | P2 | On a desktop map 330-410 px tall the open panel ends up to 53 px below the top of the (i)/Home column: the clamp measures the control's height but not where it starts (below the brand control at y 80), so `spare` is ~70 px too generous and the fold is never reached. Pre-existing: production 2.13.0 does the same (-50 px). The buttons stay clickable (painted above); the meta lines are hidden. | FIX: measure the control's bottom relative to the map (its offset within the corner), so the room rule holds as the plan states |
| A-P3-1 | P3 | The coral pointer is 2.6:1 on the cream ribbon (WCAG 1.4.11 asks 3:1 for UI parts); every text and the other UI parts pass (navy 13:1, meta 7.9:1, teal 3.7-3.9:1). | FIX: a solid 1 px navy edge on the pointer and its line |
| A-P3-2 | P3 | For an unusable zone the ribbon falls back to UTC while the page's clock falls back to the computer's zone, so the folded line could read UTC's weekday on the computer's clock. Not reachable today (the server always passes a valid IANA name; all 169 station zones are accepted by Intl). | FIX: fall back to the computer's zone like the page |
| A-P3-3 | P3 | Speed selector: no `aria-controls` / listbox `id`; ArrowUp with the listbox itself focused (no option) lands on the middle option. Everything else of the pattern confirmed right. | FIX |
| A-P3-4 | P3 | Coarse-pointer targets: the toggle (~20 x 17 px) and the overview slider (14 px) are below 24 px; the play / speed / options are 40-44 px. | FIX: 24 px minimums on coarse pointers |
| A-P3-5 | P3 | `aria-live="polite"` on the whole panel host: the ribbon label (and the folded line) change on every frame, 2-8 times a second while playing, so a screen reader is read the time continuously. Pre-existing in kind (the old Valid line). | FIX: live regions only on the warning lines; the focused ribbon's `aria-valuetext` carries the time |
| A-P3-6 | P3 | Test gaps from A's mutants (table below). | FIX the real ones with pins |
| A-P3-7 | P3 | Nits: 9 px tick labels (the mockup's); "Next Update" refreshes only on frame changes or the 30-min run check, so "about 1:35 PM" can outlive the time by up to 30 min on an idle panel; the pinned first-day label drop (F3) leaves a run that starts up to 15 h before midnight without its own date. | 9 px: ACCEPT (the mockup); the run line: FIX with a one-minute refresh while mounted; the drop: ACCEPT (by design) |
| B-2 | P3 | Keyboard focus is dropped by the fold/unfold render (the toggle's click re-renders and nothing refocuses the new toggle); pre-existing on production. | FIX: refocus the new toggle when the old one had focus |
| B-3 | P3 | = K-1: the wind layer's head names the wave model ("GFS-Wave (WAVEWATCH III)"), as in the mockup; the attribution says "NOAA GFS-Wave/GFS". | ACCEPT (owner awareness; a one-line refinement if wanted) |

Notes from B, not findings: in a DST-repeated hour the ribbon label prints the same clock for two frames an hour apart
(inherent to a 12-hour clock; the positions and `aria-valuetext` differ); Escape on the ribbon does not stop a running
map tool (the old slider behaved the same); 4x playback cannot reach 125 ms per frame without a GPU (headless software
rendering, both sites alike; with the pane's GPU both run 130 ms).

## Confirmed by the reviewers (the evidence is in their reports)
- Geometry: `ribbonLayout` / `localMidnightBefore` / `ribbonFormatter` against an independent 15-minute Intl scan in 16
  zones (Honolulu, UTC, Kolkata, Kathmandu +5:45, Apia +13, Kiritimati +14, Etc/GMT±, New York both changes, London both,
  Auckland, Lord Howe 30-min both ways, Chatham +12:45/+13:45 both, Easter) with 209 and 81 frames at four scales, 1-3-frame
  and trimmed manifests: identical except A-P2-1. `RibbonState`: 1,022,273 checks (nearest ties later, monotone slow drags,
  the 4 px tap rule, clamps, wheel).
- The scrub path vs the frame pipeline: staged races (12-stop drags, 10 late out-of-order answers, 404 / cooldown frames,
  field switch and Update mid-scrub, play and keys during a pending scrub, unmount mid-scrub) and 300 random sequences of
  20-60 operations: the picture is the labelled frame whenever no state is shown, in flight <= 2, cache <= 5, no record of
  another run, nothing scheduled after unmount.
- Keys clamp at both ends with and without unavailable end frames; playback still wraps. Resize watchers: zero listener /
  observer growth over 20 cycles, one handling per size, Leaflet 1.9.4 read to confirm the map's `resize` never fires on
  this page. Folds: the reviewer's own thresholds match the arithmetic; `collapsed` never set by a size fold.
- The selector's listbox pattern, one document listener while open and none closed over 50 renders, outside press cannot
  be swallowed by Leaflet's propagation stop. `dotMonth` on real ICU output in nine locales. The five-file asset pin, the
  PNGs (RGBA, no metadata), the flag-off page byte-identical to the golden, CI runs every overlay suite, no `innerHTML`.
- Test site (B): the owner's brief item by item on 2.14.3 and 2.14.4 — palette, no heading, one play button, the ribbon
  with real dates / ticks / 3-hourly spacing, the weekday label, the overview slider, no visible speed number, the animal
  moving only while playing, the three meta lines, the legend's LUT bytes and tick positions IDENTICAL to production's
  for hs / tp / wind, the open head and the folded "Thu, Oct. 8, 03:00 AM HST"; layers and Off (0 requests after Off);
  real drags / taps / wheel / keys with the map never moving; loading, unavailable and out-of-order states; playback
  cadence 506-511 / 253-262 / 130 ms; the loop; forecast-point and live-buoy clicks during playback; the menu upward on
  phones; sizes 1280x800 -> 375x812 -> 812x375 -> 812x260 -> 360x640 -> 412x915 -> 600/570x800 -> 1280x360/330/300/260
  without a reload, the selected time exactly under the pointer at every size, the viewer's fold sticking across sizes;
  zones (UTC, New York) and units; warm first-frame times within noise of production; no long-task regression; no memory
  growth over 10 Off/On cycles; a clean console; accessibility names / roles / Tab order; reduced motion emulated (no
  motion, no direction fetches); "expected shortly" and "a newer run is available" through the module's own paths.

## Could not check (both reviewers)
A real phone or Safari (K-6); a DST clock change on the live site (today's runs end 2026-10-23); a real hidden tab in the
pane; the desktop menu's upward flip (unreachable on this page: the window is a sheet before the menu could reach the
bottom); whether a real device delivers the P2-2 / P2-3 event timings.

## Mutation (reviewer A)
Reviewer A re-ran the author's step scripts in its own copy at 6db621b: step 1 26 of 28 applied, step 2 23 of 24, step 3
22 of 23, step 4 6 of 6, step 4b 26 of 26, step 4c 14 of 14, step 5a 8 of 8 (the not-applicable ones were written for code a
later step rewrote). The four survivors are all EQUIVALENT: E2 (the `% 24` guard is inert with `hourCycle: 'h23'`), R2
(`<` for `<=` in the bisection still brackets the tie), P16 (`_syncUI` already removes `.ov-playing` at unmount's pause),
W3 (`seek()` carries the same guard). No real gap. NOT FINISHED: A's own 56 mutants (`g26/a/mut/mut_mine.py`) were cut
off by two app restarts after the first one; the fix round's own mutants (below) and the re-check cover the changed code.

## Fix round scope (step 6; asset 2.14.5)
1. `ribbonLayout`: a day label when the local date changes (A-P2-1); tests for the Azores / Havana / Santiago runs.
2. `_buildRibbon`: `release()` drops the queued rAF scrub (A-P2-2); `pointercancel` ends without the tap rule and the
   ribbon gets `touch-action: none` (A-P2-3); tests from A's `panel_extra.test.js`.
3. `render()`: the desktop room rule from the control's bottom relative to the map (B-1); the toggle refocused after a
   fold / unfold when it had focus (B-2); live regions only on the warning lines (A-P3-5); a one-minute run-line refresh
   while mounted (A-P3-7).
4. The selector: `aria-controls` + listbox `id`, ArrowUp from the listbox to the last option (A-P3-3).
5. `ribbonFormatter`: the computer's zone as the fallback (A-P3-2).
6. CSS: a navy edge on the coral pointer and line (A-P3-1); 24 px coarse targets for the toggle and the slider (A-P3-4).
7. Pins for A's real mutation survivors (A-P3-6); the version bump, the asset pin, README; test site; a short fresh
   re-check at MAX; then STOP for the owner's production approval (tag `prod-pre-panel` @ 8f73527).
Accepted: 9 px tick labels; the F3 first-day drop; K-1 (the wind head); B's notes.

## The fix round (step 6, Opus 5.5 HIGH): asset 2.14.5
Every item of the scope above was fixed, each with a test:
- A-P2-1: `ribbonLayout` labels a day where the local DATE changes (one label per date, at its first hour) and takes one
  tick per local hour, so a clock change at midnight neither drops a day (Havana / Azores / Santiago / Beirut / Cairo
  spring) nor labels one twice (Havana / Azores autumn). The author's independent check now differs only on each run's own
  first day, which the accepted 60 px rule drops for runs starting 1-5 h before local midnight (as in New York).
- A-P2-2: `release()` drops the scrub queued for the next frame before its own. A-P2-3: only a `pointerup` applies the
  tap rule; `pointercancel` and `lostpointercapture` snap to the frame under the pointer; the ribbon is `touch-action: none`.
  Reviewer A's two repros, ported, now pass.
- B-1: the desktop room rule measures the control's top within the map (`_ctlTop`; 10 px without layout), and a desktop
  height change re-renders the panel. B-2: the toggle keeps the keyboard focus through a fold. A-P3-5: the ready panel is
  no longer a live region (only loading / error and the unavailable note). A-P3-7: a one-minute run-line refresh while
  mounted. A-P3-3: `aria-controls` + `ovSpeedMenu`; ArrowUp from the list to the last option. A-P3-2: an empty or unknown
  zone reads as the computer's own zone (the no-Intl fallback reads the computer's clock). A-P3-1: a solid navy edge on
  the coral pointer and line. A-P3-4: 28 px toggle and 24 px slider on coarse pointers.
- Tests: overlay +1 (midnight clock changes in five zones), playback +1 (the minute timer), panel +6 (the two touch races,
  the control-top room rule with the map off the page's top, toggle focus, live regions, the selector's ARIA), updated
  expectations (the formatter fallback; a desktop height change now re-renders). Mutation: 19 of 19 killed
  (`scratch panel/mut_step6.py`; B2 survived first, killed by placing the map 50 px down the page in the test).

## Test-site check of the fix round (asset 2.14.5, headless Edge with trusted touch input)
- Confirmed: the ribbon's `touch-action` is none and the pointer's line has the navy edge; a tap with a 2 px roll lands on
  the frame under the finger at lift-off and stays there; at 1280x460 / 420 the panel ends 18 px above the (i)/Home column
  and at 1280x400 it drops the details and keeps the transport row (was up to 53 px under it); the folded line "Wed, Oct.
  7, 04:00 PM HST" with the focus kept on the toggle; the ready panel not live; no page errors.
- FOUND: with `touch-action: none` the browser no longer cancels a vertical swipe on the ribbon, so it ended as an ordinary
  lift-off with < 4 px sideways travel and the tap rule moved the time 8 h. FIXED in asset 2.14.6: a press that travelled
  10 px or more up or down (tracked during the move and at the lift-off) is never a tap; it snaps back to its frame. Panel
  test (a straight swipe, an up-and-back swipe, a tap with a 6 px wobble still a tap); 4 of 4 mutants killed.
