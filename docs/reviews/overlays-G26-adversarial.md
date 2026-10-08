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
- 2.14.6 on the test site: the vertical swipe no longer jumps 8 h, but its 2 px of sideways drift (0.56 h at 3.6 px/h) still
  snapped it to the NEIGHBOURING frame (16 -> 15). FIXED in asset 2.14.7: `RibbonState.end` sends a release that is not
  a pick (no finger position) with < 4 px of travel back to the offset it started from; a real drag still snaps to the
  nearest frame. Unit test; the old `end` fails it.

## Re-check of the fix round (step 6, Opus 5.5 MAX): asset 2.14.7, two fresh reviewers
Reviewer R1 read the code without a browser. It worked in its own worktrees, re-ran A's scripts, ran an all-zone check,
built a gesture case table with a 6,000-gesture fuzz, and ran three mutant sets. Reviewer R2 tested the test site
(test @ ececc24) with trusted input in headless Edge 154: touch with emulation on the phone sizes, and mouse, wheel and
keys on desktop. It logged `isTrusted` on every event. Their reports are in scratch `g26/recheck/r1/recheck-r1-report.md`
and `g26/recheck/r2/recheck-r2-report.md`.

**Result: 0 P0, 0 P1, 3 P2, 6 P3 distinct.** R1 found 0 P0, 0 P1, 2 P2 and 7 P3; R2 found 0 P0, 0 P1, 2 P2 and 1 P3;
three findings overlap. Neither reviewer saw wrong data under a label, a hang or a console error. The map never moved
under a ribbon gesture, and nothing covered the (i)/Home column at any size.

### The G26 findings
| Finding | Verdict | Evidence |
|---|---|---|
| A-P2-1 clock change at midnight | FIXED | R1 checked every UTC-offset change of 2026-27 (520 changes in 130 zones), each with 20 run starts: 65,920 + 3,352 layouts, 0 differences from an independent 15-minute scan. R2 checked 780 layouts in Edge's own Intl (0 mismatches) and switched the live page to Atlantic/Azores; the 25-hour day showed one "Oct 25" label. |
| A-P2-2 tap race | FIXED | R2: 36 of 36 trusted taps with a 1-3 px roll landed on the tapped frame. With rAF forced 80 ms late, 6 of 6 made one seek only. R1: A's TAP RACE passes in both orders; nothing landed after a release in the fuzz. |
| A-P2-3 cancel / vertical swipe | PARTLY | Fixed: a real touch cancel goes back to its own frame, and straight swipes leave the time alone (R2, 36 of 36). Left: a swipe that drifts sideways still moves the time (RC-1). |
| B-1 desktop room | FIXED | R2 swept 1280 x 330-900 in 10 px steps (58 fresh loads, 232 resize steps, open and folded): never an overlap, minimum clearance 20 px. At 1280x400 the clearance is +42 px (was -53). R1's arithmetic holds at both edges for control tops 0, 10, 80 and 140. |
| A-P3-1 pointer contrast | FIXED | R2's 2x pixels show a 1 px navy edge on each side of the coral line and on both slanted edges of the triangle; navy on cream is 12.91:1. |
| A-P3-2 zone fallback | FIXED | R1: 924 checks (7 computer zones x 22 zone values x 6 instants) all match the page's clock. R2 confirmed it in the browser with the computer's zone emulated. A visitor cannot reach this path. |
| A-P3-3 selector | FIXED | Both: one `#ovSpeedMenu` across renders; ArrowUp from the listbox goes to the last option. |
| A-P3-4 coarse targets | FIXED | R2 measured on coarse-pointer phones: toggle 28 x 28 px, overview slider 24 px tall. |
| A-P3-5 live regions | PARTLY | The ready panel is no longer live, but the unavailable note, still a live region, is rewritten on every frame (RC-7). |
| A-P3-6 test gaps | FIXED as scoped | The re-check found new gaps (RC-8). |
| A-P3-7 run line | FIXED | R2: the real timer fires every 60.0 s on an idle panel; with the clock faked +6 h it switched to "expected shortly". R1: one timer per mount, cleared at Off. |
| B-2 focus through a fold | FIXED for the toggle | Other renders still drop the focus (RC-6). An unfold that ends with no toggle drops it too (RC-3). |

### New findings
| # | Sev | Finding | From | Author's check |
|---|---|---|---|---|
| RC-1 | P2 | A vertical swipe on the ribbon that drifts 4 px or more sideways is handled as a drag. Over a 100-120 px swipe, 4 / 6 / 8 / 12 px of drift moves the time 1-4 h (R2: trusted touch, 3 of 3 each, on two phone sizes and with a mouse; R1: the case table). Swipes at 70-80 degrees scrub 6-10 h. A straight swipe while playing stops playback. There is no direction lock; the 4 px tap line is the only guard. R2 tried `touch-action: pan-y` again: no `pointercancel` in 48 trials, so reverting would not help. | R1-1, R2-1 | Code: the moves follow x from the first move, and `end()` keeps any travel of 4 px or more. |
| RC-2 | P2 | Two fingers on the ribbon move the time by up to 1.5 days. Every `pointerdown` restarts the drag, and moves are not filtered by `pointerId`. A pinch-out gave +33 h, a pinch-in +17 / +28 h, and a second finger during a drag jumped +46 -> +64 -> +51 h. Present since step 3. | R2-2 (P2), R1-5 (P3) | Code read. |
| RC-3 | P2 | At some sizes the folded panel offers ▸ but the open panel cannot fit even the transport row. Tapping ▸ then removes ▸, opens nothing, and takes the keyboard focus with it. On the phone sheet this is maps of about 195-274 px: a phone held sideways with the forecast or live-buoy bar docked. On desktop it happens in the page's phone mode. Present since step 4c. | R1-2 | Re-run on the author's code: at maps of 195, 230 and 264 px, ▸ disappears, `collapsed` becomes false and the focus stays on the removed toggle; at 274 px the transport row opens. |
| RC-4 | P3 | Even with less than 4 px of drift, a swipe loads the neighbouring frame and then goes back (472 of 1,838 swipes in R1's fuzz; R2 saw seeks 39, 38, 39, 40). | R1-3, R2 | Code read. |
| RC-5 | P3 | The tap rule counts the finger's roll twice: it uses `o0 +` the lift-off point, but the ribbon already followed the roll. A 2 px roll at a frame boundary picks the next hour. | R1-4 | Code read. |
| RC-6 | P3 | Every render except the toggle's own drops the keyboard focus, and an open speed menu closes. Since B-1, every desktop height change re-renders: a window resize, page zoom, a browser bar, or the page's phone-mode bars. | R1-7, R2-3 | Code read (`clear(host)`). |
| RC-7 | P3 | The unavailable note, a live region, is rewritten with the same text on every frame. A loading render makes the host live in the same step as its content, which screen readers often miss. | R1-6 | Code read: `_syncUI` writes `textContent` on every call. |
| RC-8 | P3 | Test gaps. Mutants of these survive the shipped suites: (a) the `!rb.dragging` guard, the only thing stopping the `lostpointercapture` that follows every `pointerup` from undoing a tap (S4); (b) the pointerup-only tap rule (G7); (c) the 10 px swipe threshold (G3); (d) the zone-fallback tests, which only fail off-UTC while CI runs in UTC; (e) the first height change after binding; (f) the order of the B-2 focus check; (g) the timer period; (h) the CSS edge and coarse sizes; (i) the roll rule. Also pre-existing gaps nothing kills: U2, T6, S8, A5. | R1-8 | S4, G7 and G3 run against the shipped suites: all three survive. |
| RC-9 | P3 | `FMT_CACHE` is a plain object, so `ribbonFormatter('constructor')` returns `Object` and `ribbonLayout` throws. Not reachable: the server always sends a validated IANA name. | R1-9 | Code read. |

Not counted:
- K-5 is confirmed, along with two more stale comments of the same kind: `_onSize`'s header and `ribbonLayout`'s
  "every local midnight".
- Turning the layer Off in the loading or error state leaves `aria-live` on the empty panel.
- By design: the 4 px tap line is below a finger's usual slop, so a 4-9 px slide is a 1-2 h drag.

### Confirmed by the re-check
- **Suites:** Node 460 and pytest 825 pass in R1's worktree.
- **A's scripts at 630a768:** the scrub fuzz passes 8 of 8; the RibbonState fuzz passes 1,022,273 checks; the panel
  extras pass 15 of 16, and the one failure is by design (a desktop height change now re-renders).
- **K-2:** not worse than stated.
- **Touch:** 228 one-finger gestures on two phone sizes (taps, rolls, wobble up to 9 px, long press, short and long
  drags, clamps at 0 / 208). The map never moved, and the page never scrolled or zoomed.
- **Mouse, wheel and keys:** as in G26.
- **Sizes:** nine sizes without reloads. The time sits exactly under the pointer, nothing covers (i)/Home, and the
  viewer's fold is kept.
- **Regression sweep:**
  - fields, and Off with 0 model requests afterwards;
  - cadence and the loop;
  - loading, 404 and out-of-order frames;
  - the folded line "Fri, Oct. 9, 08:00 AM HST";
  - zones and units;
  - forecast-point and live-buoy clicks while playing;
  - the phone menu opening upward;
  - a fresh tab with no console errors;
  - the warm first frame within noise of production (286 vs 305 ms).

### Mutation
- The author's step-6 set: 18 of 18 applicable killed.
- R1's own 50: 28 of 49 applied killed. Each of the 21 survivors has a verdict; the real gaps are in RC-8.
- A's 56 (K-6, now finished): 32 of 51 applicable killed. U2, T6, S8 and A5 are killed by nothing.

### Could not check
- A real phone, Safari or Firefox, or a screen reader.
- A real device's cancel timing and how far a real finger drifts sideways.
- The in-app pane (hidden).
- A clock change at midnight on the live run (checked through the module instead).

### Fix round 2 scope (asset 2.14.8)
1. **The ribbon's gestures (RC-1, RC-2, RC-4, RC-5).**
   - One pointer per gesture: the pointer that pressed.
   - Its direction is decided once the press has travelled 10 px, by whichever direction is larger. Sideways is a
     scrub: playback pauses and scrubbing starts then. Up or down is a swipe: never a pick, the ribbon goes back, no
     frame is loaded, and playback carries on.
   - Before the decision the ribbon follows the finger but loads nothing. A release before it is a tap if the finger
     moved less than 4 px sideways (the frame under the finger when it touched down), or a short drag otherwise.
   - A second finger ends the gesture as no pick: back to the frame it started on.
   - The tap uses the current offset plus the lift-off point.
   - Fix K-5 and the two other stale comments.
2. **The fold (RC-3).**
   - Decide open or folded from the built panel, measured.
   - Offer ▸ only when opening shows more than the one-line head. The sheet opens whenever the transport row fits;
     this replaces the fixed 110 px rule.
   - When a focused toggle goes, move the focus to the head play button.
3. **Focus through renders (RC-6).** After any render, the control that had the focus gets it back on its successor
   (with preventScroll): toggle, play, ribbon, speed button or menu, overview slider, Update or Retry. This replaces the
   toggle's own refocus.
4. **Announcements (RC-7).** One persistent, visually hidden status node (role=status) carries the loading, error and
   unavailable messages, and is written only when the text changes. The host and the note are no longer live regions,
   so Off leaves nothing live.
5. **`FMT_CACHE`:** prefix its keys (RC-9).
6. **Tests and release.**
   - Tests for RC-8 (a)-(i) and for U2, T6, S8 and A5, plus mutants of the new code.
   - Version 2.14.8, the asset pin and README.
   - The test site with trusted touch (headless Edge) at MAX, then a short fresh check of the gesture model.
   - Then STOP for the owner's production approval.

Accepted: the 4 px tap line (by design), and K-1 to K-6 as stated.

## Fix round 2 (step 6, Opus 5.5 HIGH): asset 2.14.8
Every item of the scope above was fixed, each with a test:
- **Gestures (RC-1, RC-2, RC-4, RC-5).** One pointer per gesture. Its direction is decided at 10 px of travel by the
  larger direction: sideways scrubs (playback pauses then), up or down is a swipe that loads nothing and leaves playback
  alone. Before the decision the ribbon follows the finger but loads nothing. A second pointer ends the gesture as no
  pick, back to its start frame. A press from the same pointer as an unfinished gesture starts over. The tap is the
  current offset plus the lift-off point. A cancel before the decision is no pick. The stale comments are gone.
- **Fold (RC-3).** The open panel is always built and measured. The toggle is offered only when opening shows more than
  the head line. The size default is "as much as fits" (the fixed 110 px rule is gone).
- **Focus (RC-6).** `_focusedPart` / `_refocus` carry the focused control across every render; the toggle's own refocus
  is gone.
- **Announcements (RC-7).** One `role=status` node (`_say`) holds the loading, error and unavailable messages and is
  written only when the text changes. The unavailable note is written only on change. The panel has no `aria-live`.
- **RC-9.** `FMT_CACHE` keys carry a `z:` prefix.
- **Tests.**
  - overlay +1: roll and `cancel`. The zone-fallback test now runs in a non-UTC zone and pins Object-member zone names.
  - panel +8: drifting swipes, angles, no load before the decision, two pointers, foreign pointers, a release then
    lostpointercapture, a cancel, the 9 / 10 px boundary, the roll, the measured fold, focus through renders, the status
    node, the first height change after binding, the line-mode wheel, the listbox tab stop.
  - Updated: the measured sheet, the pause at the decision.
  - playback: the timer period.
  - CSS pins: the navy edge, the coarse sizes, `.ov-sr`.
  - Flag needles.
- **Results.** Mutation: 32 of 32 killed (scratch `g26/fix2/mut_fix2.py`; 4 survived the first run and were pinned).
  pytest 825 (exit 0), Node 469, and the Node suites also pass under TZ=UTC.

## Test-site verification of fix round 2 (Opus 5.5 MAX): asset 2.14.8 on test @ 8ff91b8
- **Deploy.** test @ 8ff91b8 is the cherry-picks of the re-check record and fix round 2 (README kept as test's). Live
  from 04:19 UTC. The served `overlay.js` and `overlay.css` equal the committed files byte for byte; the 2.14.7 URL
  answers 404. Every mount reported 2.14.8.
- **Method.** Headless Edge 154 with trusted DevTools input (scratch `g26/verify2/hl/`, reviewer R2's harness on my own
  port and profile): touch with emulation and a coarse pointer on the phone sizes; mouse, wheel and keys on desktop.
- **Touch, 375x812 and 412x915: 280 gestures, 0 flagged.** Taps and taps with a 2 / 3 / 3.5 px roll land on the frame
  where the finger touched down. Wobble up to 9 px is a tap; 10 / 12 px is no pick. These all leave the time unchanged
  and load no frame: straight swipes, swipes drifting 3 / 4 / 6 / 8 / 12 / 20 px, a swipe drifting 6 px down, a drift
  that comes back, diagonals at 50 / 60 / 70 / 80 degrees, and 12 px up followed by 60 px sideways. Diagonals at
  20 / 30 / 40 degrees scrub to the expected frame, as do short drags of 4-12 px, the slow 60 px drag (+17 h), the clamps
  at 0 and 208, a long press, and a 30 px drag that is then cancelled. A 2 px roll then a cancel is no pick. While
  playing at 1x, a straight swipe, an 8 px drifting swipe and a 70 degree swipe keep playback running with no seek; a tap
  and a drag pause. The map never moved, the page never scrolled or zoomed, and no contextmenu appeared.
- **Two fingers** (3 runs each, all as designed):
  - a second finger landing during a drag goes back to the start frame (seeks 43, 44, 46, 40);
  - a pinch-out and a pinch-in on the ribbon, and a second finger tapping while the first holds: unchanged, no seek;
  - one finger on the map and one on the ribbon: an ordinary one-finger gesture.
- **Fold (RC-3): 100 sizes, 0 dead toggles, nothing over (i)/Home.**
  - Phone sideways (812 x 240-430), shrinking and growing: the play row with a working toggle from a 317 px map up, one
    line + play without a toggle below.
  - The viewer's fold: the toggle shows exactly where opening shows more.
  - Desktop 1280 x 330-520 in the page's phone mode: the toggle works at every height, open and folded.
  - A focused toggle that disappears hands the focus to the play button.
- **Focus (RC-6), real keys.**
  - The ribbon, reached with 18 real Tabs, keeps the focus through 800 -> 700 -> 800 height changes, and ArrowRight /
    ArrowLeft still step it.
  - The play button keeps the focus and Space still plays and pauses.
  - An open speed menu closes on a height change with the focus on its button, and Enter reopens it.
  - The toggle and the overview slider keep the focus; a width-only change keeps it; the field select (outside the
    panel) is never taken.
- **Announcements (RC-7).**
  - The status node read "Loading model frame…" then "" at the mount; there is exactly one, in the body, and the panel
    never has `aria-live`.
  - With 404s injected at +42 h and +44 h during 7 s of 2x playback, it changed twice (once per new unavailable frame),
    not on every frame, and the visible note matches it.
  - With every frame 503, the error is said once; a real click on Retry gives "Loading model frame…" then "".
  - Off clears it.
- **Regression sweep** (R2's script, unchanged):
  - the open head and the folded line "Wed, Oct. 7, 07:00 PM HST";
  - every field, then Off with 0 model requests in the next 7 s;
  - cadence 513 / 257 / 147 ms at 1x / 2x / 4x and the loop wrapping at the end;
  - a 2.5 s frame shows "loading…", a 404 frame is skipped and listed, and the late frame of an out-of-order pair is
    never drawn;
  - zones and units; a forecast-point click while playing; the phone speed menu opening upward.
- **Other checks.**
  - Nine sizes without reloads, open and folded: the selected time sits exactly under the pointer and nothing covers
    (i)/Home.
  - A resize in the middle of a drag: the moves that follow never pan the map (K-2, as on 2.14.7).
  - The desktop height sweep (232 steps): minimum clearance 20 px, the same as 2.14.7.
  - A fresh tab has no console errors.
- **Mouse, by design.**
  - A click with a 2 px roll now picks the frame under the press (RC-5).
  - R2's zig-zag drags whose first move is 5 px sideways and 10 / 15 px down are now swipes. Their first 10 px are mostly
    vertical, so the larger-axis rule decides at that point. A gentle 6 px wobble and a drag drifting 15 px down still
    scrub (+16 h).
- **Screenshots:** scratch `g26/verify2/shots/overlay_panel_2.14.8.jpg`.
- **Cleanup:** the test origin's storage emptied through /robots.txt, emulation reset, Edge closed (no process left on
  the profile), and the profile removed.

## Short fresh check of fix round 2 (Opus 5.5 MAX, one fresh reviewer): asset 2.14.8
The reviewer worked on the code in its own worktree and on the test site with trusted input (headless Edge 154).
Report: scratch `g26/fix2check/fix2check-report.md` (scripts in `fuzz/` and `hl/`, raw output in `out/`).

**Result: 0 P0, 0 P1, 0 P2, 7 P3.** It saw no wrong data under a label, no hang, no console error and no stuck state.

### Confirmed by the reviewer
- **Served files:** byte for byte the commit. Its own suites: Node 469 and pytest 825.
- **Gesture fuzz:** 20,000 runs and 98,247 gestures on the real `render()`, with a controllable rAF and timers, against
  an oracle written from the brief. The events included one to three pointers, cancels, stray lost captures, renders
  mid-gesture, playback ticks, wheel, keys and same-id re-presses. Invariants (a), (b), (d), (e), (f) and (g) always
  hold; (c) holds except F1. As a sanity check, the same fuzz flags 22 violation classes on 2.14.7.
- **Live gestures:** fast flicks, a slow jittery drag, paths crossing 45 degrees, long presses at 4x then a tap (the
  frame under the finger), and two-finger taps all behaved as designed. K-2 is no worse.
- **Fold vs 2.14.7:** 108 rows, 36 sizes x 3 passes. Only 4 differences, all the intended removal of the dead toggle
  (RC-3). Nothing over (i)/Home.
- **Focus:** never taken from outside the panel. It follows the panel between the control and the sheet, and never
  scrolls the page.
- **Status node:** one node through Off / On, errors, Retry and Update. It does not change the page's scroll size or
  print, and it speaks once per new unavailable frame.
- **`FMT_CACHE`:** Object-member zone names no longer throw.

### Findings (author's check in the last column)
| # | Sev | Finding | Author's check |
|---|---|---|---|
| F1 | P3 | At either end of the run, a tap whose finger rolls outward picks the frame under the LIFT-OFF point, not the touch-down point. `rb.move` clamps the offset, so the current offset plus the lift-off point counts the clamped part of the roll. Live: 10 of 25 taps at +0 h with a rightward roll went 1-2 frames late; 0 of 6 at frame 40. | Reproduced on `RibbonState`: frame 0 with 1.26 px + 1 px roll picks 1 (touch-down 0); 0.72 + 2 picks 1; 2.34 + 3.5 picks 2 (1); the last frame with -1.26 - 1 picks 119 (120); frame 40 correct. |
| F2 | P3 | Keyboard focus is lost through the loading and error renders. Their early returns never call `_refocus`, and the loading panel has nothing focusable, so the ready render finds the focus outside the panel. Live with real keys: Retry focused + a height change -> BODY; Enter on Retry (failing or succeeding) -> BODY; Enter on Update -> BODY. Fix round 2's scope listed Update and Retry. Not a regression. | Code: overlay.js 2771-2777. |
| F3 | P3 | A pointerup at a new position with no move before it skips the direction rule when the sideways travel was already >= 4 px. Live, trusted CDP mouse: move (-5, +3), release at (-5, +12): seek 60 -> 61 although the travel is mostly vertical. Chromium sent no pointermove at the release point. Unknown whether a physical mouse does this. | Code: the release check at 2681 looks at the release point only when `rb.moved < 4`. |
| F4 | P3 | A focused details scroll box (Chromium makes an overflowing scroller a Tab stop, seen at 1280x430) loses its focus to the play button on a re-render: `ov-details` is not in `FOCUS_PARTS`. | Code: 2896. |
| F5 | P3 | The first message of a page load is probably not spoken: `_say` creates the role=status node already holding "Loading model frame…" in one mutation batch, and a live region that appears with its content is often skipped. Not checked with a screen reader. | Code: 2919. |
| F6 | P3 | (Also in 2.14.7.) While playing at 1x / 2x, a tap during the ribbon's 150 ms step animation picks the frame AFTER the one visibly under the finger. The press drops the transition and uses the logical offset. 2.14.8: 4 / 40 at 1x and 8 / 40 at 2x; 2.14.7 served in its place: 5 / 40 and 9 / 40. Long presses at 4x (no animation) picked the visible frame. | Code: `pointerdown` sets `transition: none`, and the tap starts from `rb.offset` (the target, not the visible position). |
| F7 | P3 | Test gaps: 16 of the reviewer's 25 new mutants survive the suites. The ones that matter: N1 (an exact 45-degree tie decides sideways), N2 / N3 (the release-point guard), N4 (`<` vs `<=` on the 4 px tap line), N5 (`back()` without `_syncUI`), N8 (the unavailable message spoken only while the details exist), N9 (the folded title keeps `.ov-model`), N15 (an empty warning box shown), N17 (direction by \|dx\| + \|dy\|), N23 / N24 (the tiny-sheet path keeps a dead toggle or drops the refocus), N25 (a wrong start frame). N5, N17 and N25 are caught only by the reviewer's fuzz. | `fix2check/fuzz/fc_mut.log.txt`. |

**Not counted (defensive):** a touch whose release never reaches the ribbon leaves the gesture open. Every later touch
(a new `pointerId`) is then taken as a second finger and ignored until the next render. The reviewer could not make a
release go missing with trusted input; capture plus `lostpointercapture` normally prevents it. Code-read confirmed.

**Noted:** a folded render now costs what an open one does (about 4.5-5.4 ms, was 0.4-1.2 ms), because the open panel
is built to be measured. Renders happen only on size changes and toggles.

### Proposed fix round 3 scope (asset 2.14.9), for the owner's decision
1. **F1:** a tap picks `o0 + (touch-down point - centre)`.
2. **F3:** the release point follows the same direction rule as a move.
3. **F6:** a press during the step animation starts from the ribbon's visible position.
4. **F2:** focus is kept through the loading and error states (a pending focus class, applied only while the focus is
   on the body) and Retry gets it back.
5. **F4:** `ov-details` is added to the focus parts.
6. **F5:** the status node is created empty and its first text written a moment later.
7. **Defensive:** a press from a new pointer starts a new gesture when the old pointer no longer holds the capture.
8. **F7:** tests for the gaps above, mutants, the version, the pin and README.
9. A short test-site check with trusted input, then STOP for production.

## Fix round 3 (Opus 5.5 HIGH): asset 2.14.9
The owner chose to fix every item of the scope above. Each fix has a test:
- **F1.** `RibbonState.end` reads a tap's touch-down point against the offset at the press (`o0 + point`), and the
  release passes the touch-down point. At frame 0 a tap 3 px right with a 3.9 px roll stays on frame 0; on the reviewer's
  four end cases the state picks the touch-down frame.
- **F3.** An undecided `pointerup` first applies the 10 px direction rule to its own point: mostly up or down is no
  pick, mostly sideways is a drag to that point. Below 10 px it is a tap or a short drag as before, and a cancel or lost
  capture is still no pick.
- **F6.** The press reads the track's computed transform (`visibleOffset`), stops the glide there and starts the
  gesture from the visible position. Without a computed transform it uses the offset, as before.
- **Lost release.** A press from a new pointer while the old gesture's pointer no longer holds the capture
  (`hasPointerCapture`, only when the capture was taken) starts a new gesture.
- **F2.**
  - A loading render keeps the focused part as `_pendingFocus`. The next render uses it only while the focus is lost
    (`_focusLost`: no element, the body, or a removed node).
  - The error render refocuses, and the fallbacks are play, the toggle, then Retry.
  - Off clears it.
- **F4.** `ov-details` is a focus part.
- **F5.** `_say` adds the status node empty and writes its first text (the latest by then) after `SAY_FIRST_MS` (100 ms).
- **Tests.**
  - overlay: the end cases of the tap.
  - panel +9:
    - F1 at the first frame; F3 swipe and drag; F6 a tap during a glide; the lost-release guard;
    - N1 / N4 / N5 / N17 / N25; F2 / F4 / N12 / N13 / N14 focus (loading, error, Retry, focus elsewhere, details,
      an unlisted part, preventScroll, an open menu with the browser's blur);
    - F5 / N8 / N21 / FS2 status; N23 / N24 the tiny sheet.
  - The N9 / N15 assertions.
  - Updated: the RC-5 contract, a realistic tap in the drag test, the async first text.
  - Needles, version, pin, README.
- **Mutation.** The round-3 set: 34 of 34 killed (scratch `g26/fix3/mut_fix3.py` + `mut_fix3b.py`; FS2 survived once and
  was pinned). The round-2 set re-run: 27 of 27 applicable killed, plus X4 and X2 re-targeted and killed. The reviewer's
  fuzz on 2.14.9 (`g26/fix3/fuzz`): 20,000 runs, 98,411 gestures, 0 violations (1,259 taps at the ends).
- **Suites.** pytest 825 (exit 0) and Node 477, also under TZ=UTC.
