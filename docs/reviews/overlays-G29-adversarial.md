# G29 — adversarial review of fast playback (plan section 42)

Reviewed: `feat/overlay-fast` @ 583455a (overlay asset 2.16.3; test site `test` @ a6c3b8f), off production
`Live-Buoy-Update` @ 908af0b (asset 2.14.12). Two fresh-context reviewers, 2026-10-13, briefed with the owner's five
decisions, the claims of steps 1-3, the author's measurements and the known items K-1..K-11 (scratch `g29/brief.md`):
- A (Fable 5.1, MAX): the code and its tests in Node, no browser. A scheduler fuzz on both decode paths (canvas and a
  FakeWorker running the real worker source; ~4,900 runs, ~390,000 operations, invariants checked after every operation),
  a node-window property test (2,000 views + 19,713 pans), a particle-pace check, hand checks and 80 mutants. Report
  scratch `g29/a/g29-a-report.md` (scripts, logs and shrunk failing sequences beside it).
- B (Opus 5.5, MAX): the live test site against production in a headless Edge with the GPU (Intel Iris Xe), trusted
  input, the decode worker's own isolate through the DevTools Target domain, timed cells with production back to back after
  >= 155-s idles, plus production readiness. Report scratch `g29/b/g29-b-report.md` (b01..b22 scripts and outputs,
  w54b_*.json, screenshots and a still sheet). The owner's Creative Cloud and Acrobat used ~20-27 % CPU during B's timed
  runs (ordinary use of this laptop, left running).
The author verified every finding below (the code read, the reviewers' raw data recomputed) before deciding its outcome.

## Result
**0 P0, 3 P1, 2 P2, 10 P3** (A: 5 P3; B: 3 P1, 2 P2, 5 P3), plus the author's own K-11 (P3). The pictures are right: the
worker and the main-thread path give byte-identical frames (8 frames, 192 tiles), the windowed particle lattice equals the
whole grid at every point the page can show, and the fuzz found no skipped landing, no frame of another run and no cap
exceeded. The defects: at 1920x1080 with particles some landed frames are never painted (against "every frame shown"),
the decode worker's own memory breaks the owner's memory rule once it is counted, the 1080p wave-height line is not reached
on this laptop in ordinary use, and two gaps on the iPad path of the particle animator.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| B-P1-1 | P1 | **Landed frames are not always painted.** At 1920x1080, shark, wave height, the GPU renders only every ~115-130 ms with particles on (both versions). `_after` (overlay.js 2416-2423) sets `due = max(due + interval, now)`, so after a late landing the successor is due at once; it is already decoded (the 4x ring is 6 ahead) and lands in the next task, before any rendering update: the first frame is replaced unseen. B's painted rule (b22: no animation-frame callback between two landings): test 6 of 180, 19 of 163, 9 of 172 unpainted on the warmed run (5 of 117 cold); production 0 of 173 / 150 / 167 (0 of 156 cold). At 1280x800 both 0; particles off 0 of 185 (rAF 17 ms). w54's "0 skipped" counts landings and could not see it. Production's `_after(t0, gen)` waits `interval - elapsed` and its 2-frame ring often fetches, which leaves the browser time to render. Author: the code read against 908af0b; b22's output re-read. | FIX: a playback landing waits for a rendering update after the previous landing (scope 1); the painted rule joins the acceptance script |
| B-P1-2 | P1 | **The owner's memory rule fails once the decode worker is counted.** Every decode in the worker allocates the body (transferred in), a concatenated IDAT copy, the stream's output chunks and the inflated bytes; the worker does almost nothing else, so V8 collects them only at its ~64 MB external-memory trigger: a sawtooth from ~2-10 MB up to ~65 MB every 1-3 s (mean 33-37 MB) in every cell, the phone included. w54 read the page isolate only. With the worker: test peaks +89..+116 MB vs production +34.5..+65.6 (allowed: production's range + 10); live over production + 10 in shark-1080-fresh (+14.3, +12.9, +12.7), shark-1280-fresh (+11.6), shark-1280-pan (+12.1). OS view (renderer private bytes, 1080p shark): production mean 198 / max 210.9 MB, test 228 / 262.5. On the page metric alone every cell passes. Author: recomputed from B's w54b JSON (same picture in MiB: test live +24.4..+40.3 vs production +9.8..+27.9, peaks +85..+111 vs +33..+63). | FIX: the worker keeps nothing per decode and hands its buffers back (scope 2); page + worker in the acceptance script |
| B-P1-3 | P1 | **The 1080p wave-height shark line was not reproduced.** shark-1080-fresh, three pairs, production -> test (fps, gap p50/p90 ms): 7.0 (135/177) -> 5.9 (153/237); 5.4 (180/232) -> 6.0 (142/280); 5.5 (181/216) -> 6.1 (141/256); shark-1080-pan 6.6 -> 7.8 (125/150.3). Test's p90 worse in all three (gap sd 59-73 ms, a late frame then a quick one: P1-1's catch-up) vs production sd 24-34. Passing in the same session: 1280 fresh / pan 8.0 (125/137-138) vs 7.6; wind 1080 7.9 (125/144) vs 7.6; phone 8.0 vs 7.6; fish 4.0 vs 3.9; snail 2.0 vs 2.0. The particle canvas's GPU raster is the main 1080p limit on this iGPU; machine state decides as much as the code (the author's 8.0 / 125 / 139 came after cool-downs with nothing else running). | RE-MEASURE after scope 1-2 in two machine states (quiet, and the owner's ordinary use) with the painted column; OWNER'S CALL if the line still misses in ordinary use |
| B-P2-1 | P2 | The watchdog's heal leaves the particle canvas hidden: a zoom whose zoomend never arrives (the iPad hypothesis) leaves `visibility: hidden` from zoomstart; the heal restarts the loop but only zoomend restores visibility, so particles are drawn invisibly while the readout says healed / active. Reproduced (b07); a flyTo does the same (b15). Author: code read (`_check` 1659-1666). | FIX (scope 4) |
| B-P2-2 | P2 | A refused 2D context on the WebKit fresh canvas takes the whole overlay down: `_newCanvas` zeroes the old canvas, then uses `getContext('2d')` unchecked; `_place` throws on `setTransform`; the TypeError reaches Leaflet's zoomend dispatch, every watchdog tick, and the next landing through `setData` -> `_tick` -> "Overlay unavailable" with Retry (3 of 3 runs, b19). Safari is reported to answer null when its canvas memory is used up, and 2.16.3 asks WebKit for a new context at every zoom. Author: code read (1637-1642, 1718). | FIX (scope 5) |
| B-P3-1 | P3 | At fish / shark the tails grow when the particle loop runs below ~50 frames/s: the tail sampling interval (66 ms / rate) cannot go below one frame, so 10 points span 10 x dt x rate of flow: equal at 60 Hz, 2x at <= 30 Hz. 1080p here: hs tail median 16.2 px paused -> 29.4 shark, wind 22.9 -> 47.8 (production 13.5 / 20.2 at shark). The owner's sheet for look D was taken at 1280x800 (~34 fps: shark tails ~1.8x). README "keep their length" holds only at >= 60 Hz. Author: code read (1797-1813). | FIX, owner's call 2026-10-13: "same length" at every speed and frame rate (scope 13) |
| B-P3-2 | P3 | Watchdog false heal right after a long hidden period: `_start` does not reset `lastFrameAt`, so the overdue check can count a heal before the first frame (measured margins 12 ms and 178 ms). Harmless to the picture, but `healed: 1` would show in the iPad readout after an app switch. Author: code read. | FIX (scope 6) |
| B-P3-3 | P3 | README "Fast playback" against the code: the 4 downloads are one budget for field and direction frames (9 decoded per kind); "tails keep their length" (B-P3-1); "keep up with the time-lapse" holds on the hourly frames (to +120 h), past that the frames are 3-hourly; "every iPad browser" (K-11); the feature test also needs URL.createObjectURL. | FIX (scope 14) |
| B-P3-4 | P3 | The `?ovdebug=1` readout is missing in the error panel (when the owner would need it, P2-2) and in the phone's landscape sheet (no details box) while its 1-s timer keeps ticking. | FIX (scope 12) |
| B-P3-5 | P3 | The acceptance script (w54_fast.js): the heap misses the worker (P1-2); "skipped" counts landings, not painted frames (P1-1); its GPU check on the page target always fails (use gpuinfo.js); the pause check's `full` compares two GPU readbacks (K-5) and is always false in GPU mode; the absolute +25 / +50 MB lines predate decision 3. B fixed the heap in its copy. | FIX (scope 16) |
| A-P3-1 | P3 | The frame on the map is protected by a key of the CURRENT resolution: after `_checkRes` switches, the old-resolution frame (and direction) still drawn has neither that key nor a rank, so a pause / slower-speed shrink or the first new-resolution landing over the cap drops it first; a zoom back across the threshold before the new frame lands downloads it again (16 of 4,900 fuzz runs; same picture throughout, one extra fetch). Pre-existing in spirit (2.14.12's LRU dropped it first too). | FIX: protect by value (scope 8) |
| A-P3-2 | P3 | The windowed lattice equals the whole grid at every in-world point (2,000 views, 19,713 covered pans); it differs only at lattice points beyond 85 N / S, which the page (84 N..79 S, a minimum zoom that fills the band) never shows. | COMMENT on `nodeWindow` (scope 10) |
| A-P3-3 | P3 | Watchdog: (a) `flyTo` sets no flag `_busy()` reads, so a flight over 2 s would be healed mid-flight (the page never calls flyTo); (b) a Leaflet flag stuck true keeps `_busy()` true forever, so that variant of the iPad fault would never heal. Verified against Leaflet 1.9.4. | (a) ACCEPT (unused; comment). (b) FIX: busy for more than 10 s in a row counts as stuck (scope 7) |
| A-P3-4 | P3 | Nits: the zoomend handler calls `_newCanvas(self.canvas.parentNode)` without a parent check (an exception inside Leaflet's `fire` skips later zoomend listeners); `pageQuery` reads a `?` inside the navigation URL's fragment; `decodeFrame` does not check `signal.aborted` before `getDecoder()` (harness-only). | FIX (scope 9) |
| A-P3-5 | P3 | Test gaps (mutants that survive the suite but describe real behaviour): P5 / P7 / C1 (the shrink keeps the drawn frame and direction outside the ring), W2 (the node window's +4 column slack is load-bearing), D3 (the main -> worker body transfer), G6 (`_startDir` at 4x must use `_maxInflight()`), G5 (a new animator created while playing takes the rate), F11 (the "healthy" watchdog test passes only because `performance.now()` < 2000 there), G2 / G4 / L6 / L7 (timers and idle callbacks after Off and in debug mode). | FIX: pins (scope 15) |
| K-11 | P3 | Author's own (in the brief): `isWebKit()` excludes CriOS / EdgiOS / FxiOS, but every iOS browser is WebKit, so Chrome / Edge / Firefox on an iPad would not get the fresh canvas after a zoom. | FIX (scope 11) |

## Confirmed by the reviewers (the evidence is in their reports)
- Picture identity (B b01): worker vs `?ovdecode=main`, run 2026101312, steps 0 / 57 / 120 / 384, wave height full + pdir
  half and wind half + wdir half: SHA-256 of every code array identical; 192 tiles' `_ovImg.data` identical; the worker was
  really used. A: the four decode functions are closure-free and the fixtures exercise every PNG filter.
- Scheduler (A's fuzz, both decode paths): in flight <= `_maxInflight()`, caches <= the ring's decoded cap, no key / drawn
  frame / direction of another run after Update, the landed sequence never skips an available frame, `flow.rate` = speed
  while playing, nothing left after unmount; all 16 failures are A-P3-1. Ring rules by hand: a 209-frame run at 4x through
  the wrap fetches every URL exactly once; a worker crash mid-playback re-downloads only the pending decodes (K-10).
  Cadence by fake clock: one playback timer, at most one back-to-back frame after a stall, never a burst.
- Worker lifecycle (B b02, b14): one worker after the first decode, none after Off (terminated, blob URL revoked), a fresh
  one after On, flat over 10 cycles, none with `?ovdecode=main`; a worker death in the browser falls back with one warning
  and playback continues (73 landings in 6 s, 0 skipped).
- Watchdog (B b03, b04, b17, b18): forced faults healed in 0.6-4 s; healed 0 through drags (slow, held, flicks), wheel /
  keyboard / box zooms, double-click, Home presets and picker focuses (animated setView), speed and field changes, reduced
  motion, 3-s and 330-s hidden windows, a simulated Update to a newer run, rotations and resizes.
- WebKit path (B b06, iPad UAs, 20 zooms at 4x): a fresh single canvas each time, old ones released, heap and DOM flat; the
  owner's play / pinch / pause sequence stops particles only during the pinch, as on production.
- `?ovdebug=1` (B b11): text only, ~1-s updates, cleared at Off, fits the phone's portrait sheet; the address rewrite does
  not hide it.
- The particle lattice from the node window = the whole grid in the live page (B b05, 11 views and their pans) and in A's
  property test; look D strokes and density as picked; pace x1 / x1 / x2 / x4; no mass respawn at the loop's wrap; heads
  rarely enter land cells.
- Fetches per frame 1.00-1.03, in flight never above the cap (also across speed / pause / seek transitions), a 5-minute
  1080p soak flat; the particle frame rate higher on test than production at every size (1080p shark 14 vs 9.3 fps).
- Production readiness (B): served overlay.js / overlay.css = 583455a, the asset pin recomputed = fixture, the test branch =
  the feature branch except 3 test-only CSS lines, no template / static_ui change (the page golden is unaffected), CI green
  on both commits, production fast-forwards from 908af0b, the tag `prod-pre-fast` not yet made (step 6).
- Suites at 583455a: Node overlay 230 pass, pytest 1,068 pass. Mutants: 80, 64 killed (2 only by the fuzz), the rest
  equivalent or listed in A-P3-5.

## Could not check
The real iPad and Safari (Safari's canvas-memory limit, its event order, the missing requestIdleCallback, its timers after
an app switch); real phones and other laptops; a fully quiet machine during B's timing; the cause of one 2.75-s stall in a
fish cell (not reproduced); Firefox; soaks longer than 5 minutes.

## Fix round scope (step 5; overlay asset 2.16.4)
1. Paint gate (B-P1-1): a playback landing waits until the browser has rendered the previous one. `_after` keeps the
   absolute due time; the tick also waits for a requestAnimationFrame callback that ran after the last landing, and runs as
   a task scheduled from that callback (so the rendering update showing the landed frame has completed). A fallback of
   ~250 ms without an animation frame proceeds anyway, so playback never stalls where frames stop. Seeks, steps, scrubs and
   the first frame of play are not gated; pause, setSpeed and unmount cancel the pending callback. Tests with a fake rAF on
   the fake clock: two landings never share a rendering frame; when rendering keeps up the cadence is unchanged (125 ms at
   4x); when it renders every 150 ms the landings follow the renders, no catch-up burst; the fallback; nothing scheduled
   after pause / unmount.
2. Worker memory (B-P1-2): the worker keeps nothing per decode. The IDAT views go to the DecompressionStream as they are
   (no concatenated copy); the stream is read chunk by chunk into one buffer of the expected (w + 1) * h bytes (more or fewer
   = decode failed); the rows are unfiltered in place (`copyWithin` for filter 0; q = the first w * h bytes) or into a new
   array; the reply transfers q's buffer and hands back every other buffer the worker received or got from the stream (the
   body, the chunks), which the main thread drops: that garbage is collected where production's decode garbage always was.
   If the browser refuses to transfer a chunk, the reply carries q alone. Tests: the PIL fixtures byte-identical through the
   new path (all five filters, both paths), the FakeWorker sees every buffer in the transfer list, short / long / corrupt
   streams fail, the main-thread fallback unchanged.
3. B-P1-3: re-measured in step 5b (below).
4. B-P2-1: the heal shows the canvas again (visibility '', on WebKit a fresh canvas as zoomend does); a flow test with a
   zoomstart that never ends.
5. B-P2-2: `_newCanvas` takes the new context before releasing the old canvas and keeps the old one when it gets null;
   nothing in the animator throws on a missing context (no particles instead); `_syncFlow` catches an animator exception
   (logged once) so it never reaches playback; `debugState` reports the context (and `isContextLost()` where it exists).
6. B-P3-2: `_start` sets `lastFrameAt`; a test with a long gap before the first frame.
7. A-P3-3 (b): a map busy for more than 10 s in a row counts as stuck; (a) a comment.
8. A-P3-1: `_trim` also protects by value the frame on the map and the animator's direction; A's shrunk sequences as tests.
9. A-P3-4: the zoomend parent check; `pageQuery` ignores a `?` inside the fragment; `decodeFrame` checks the abort before
   `getDecoder()`.
10. A-P3-2: the comment on `nodeWindow`.
11. K-11: `isWebKit()` = AppleWebKit and (no Chrome / Edg / OPR / Firefox mark, or an iOS mark: iPad / iPhone / iPod /
    CriOS / EdgiOS / FxiOS, or a Macintosh UA with touch points); the flow test's CriOS case becomes true.
12. B-P3-4: the readout in the error panel too; its timer stops while no readout element exists and restarts with the next
    render that has one.
13. B-P3-1 (owner: same length): the drawn tail is bounded by flow time: each recorded point keeps the particle's flow
    clock (dt x rate), and the tail starts at the first point within TRAIL_POINTS x TRAIL_EVERY_MS (~667 ms) of flow, so a
    tail has its paused length on screen at every speed and frame rate; nothing changes at >= 60 Hz. A test at 15 / 30 /
    60 Hz and rates 1 / 2 / 4.
14. B-P3-3: README (the shared download budget, the tails, the time-lapse sentence for the hourly frames, every iOS
    browser, the feature test, the paint gate and the worker's hand-back).
15. A-P3-5: the pins listed (A's window property test adopted as is).
16. B-P3-5: B's w54 copy (page + worker heap) adopted, the painted rule as a pass condition, the GPU check through
    gpuinfo.js, the pause check without `full` in GPU mode, the heap rule relative to production (same cell, back to back).
17. Asset 2.16.4 + fixture, the suites, mutants of every fix.
Step 5b (MAX): deploy to test; the 7 cells + phone + a particles-off diagnostic, production back to back, with the painted
column and page + worker heap, in two machine states (quiet after >= 150-s idles, and ordinary use with the owner's usual
background apps); the OS memory view; b01 identity, the heal and null-context drills re-run.
Step 5c (MAX): a short fresh re-check (the paint gate under a fake-rAF fuzz, the worker hand-back, the animator guards);
then the owner's go-ahead for production (tag `prod-pre-fast` @ 908af0b, fast-forward).

## Fix round (step 5; overlay asset 2.16.4)
Built 2026-10-13 (Fable 5.1, HIGH) on `feat/overlay-fast` after e4f0ba1; `static_overlay/overlay.js`, the three overlay Node
suites, `tests/test_overlay_flag.py`, `app.py` (the version), the asset fixture, README. Every scope item above is done:
1. **Paint gate** (`_paintWait` / `_paintClear`; `_tick` waits while `_painting`): after a playback landing the next tick runs
   from a task scheduled by the first animation frame after it (the rendering update showing the landed frame has completed
   by then), never earlier; `PAINT_FALLBACK_MS` 250 proceeds without an animation frame; the gate is off where
   `requestAnimationFrame` does not exist; play, pause and Off clear it (seeks, steps and scrubs are not gated). Fake-rAF tests:
   two landings never share a rendering frame, nothing lands inside the callback itself, a render that keeps up leaves the
   125-ms cadence alone, a 150-ms render paces one landing per render with no burst, the fallback, nothing left after Off.
2. **The worker keeps nothing**: `parsePng` returns the IDAT views (no concatenated copy); `inflate(parts, size, spent)` writes
   them to the inflater as they are and reads its output into one `(w + 1) * h` buffer (more or fewer bytes = decode failed);
   `unfilter` works in place (the code array is a view of the first `w * h` bytes); the worker's reply transfers the code
   array's buffer AND the body and every inflater chunk (`spent`, dropped on the main thread; a transfer the browser refuses
   falls back to the code array alone). The main-thread path gains the same (no second copy). Pinned: the fixtures byte for
   byte through both paths, the transfer lists, short / long streams refused in both paths, the main -> worker body transfer (D3).
3. B-P1-3: re-measured in step 5b.
4. **B-P2-1**: the heal shows the canvas (`visibility = ''`; a fresh canvas on WebKit or when the canvas has no context).
5. **B-P2-2**: `_newCanvas` takes the new context first and keeps the old canvas when it is refused; `_place`, `_start`,
   `_clear`, `_renderParticles` do nothing without a context (the watchdog then retries the canvas every period); `_flowData`
   wraps the animator's `setData` (one warning, the playback goes on); `debugState` reports `ctx` / `ctxLost`.
6. **B-P3-2**: `_start` records `lastFrameAt`. 7. **A-P3-3 (b)**: `BUSY_STUCK_MS` 10 s: a map busy that long in one go counts
   as stuck (reset after a heal); (a) a comment. 8. **A-P3-1**: `FrameCache.set/limit/_trim` take `keepValue` (the frame on
   the map / the animator's direction by identity); `_prefetch` and the landing pass them; the room rule reserves a slot
   while the drawn frame or direction is kept under another resolution's key. 9. **A-P3-4**: the zoomend parent guard;
   `pageQuery` strips the fragment before the `?`; `decodeFrame` checks the abort before `getDecoder()`. 10. **A-P3-2**: the
   comment on `nodeWindow`. 11. **K-11**: `isWebKit()` = AppleWebKit and (an iOS mark iPad / iPhone / iPod / CriOS / EdgiOS /
   FxiOS, or a Macintosh UA with `maxTouchPoints > 1`, or no Chrome / Chromium / Edg / OPR / Firefox mark). 12. **B-P3-4**:
   the readout is drawn in the error panel too; its timer stops when no connected readout element exists and the next render
   restarts it. 13. **B-P3-1 (owner: same length)**: each tail point carries the particle's flow clock (`histF` / `flowT`);
   points older than `TRAIL_SPAN_MS` = 11 x 66 ms of flow are left out, so the tail spans the same flow time at 60 / 30 / 15 Hz
   and x1 / x2 / x4 (test: within 0.7-1.2 of the 60 Hz x1 tail in all nine cells; the old rule's 30 Hz x4 tail was ~1.8x).
   14. README. 15. **A-P3-5 pins**: P5 / P7 / C1, W2 (A's property test, 120 views + their covered pans, adopted), D3, G6,
   G5, F11, L6 / L7 / G2 / G4 (the fake-timer queue, intervals included, is empty after Off; the readout's timer stops itself).
   16. The acceptance script: reviewer B's `w54_fast.js` (page + worker heap over flattened worker sessions) adopted into
   `wind/verify5/vhl/`, plus the painted rule as a pass condition (`unpainted N/M (render p50)` on every line), the heap
   lines reported for the relative rule, the GPU-readback `full` dropped from the pass condition (K-5). 17. Asset 2.16.4,
   the fixture, needles.
- Suites: Node overlay 243 (was 230), `tests/test_overlay_flag.py` 11, full pytest below. Mutation pass of the fixes (scratch
  `fast/mut4.py`, 32 mutants, the file restored from a byte copy): 29 killed, 3 survive as double guards: M5 (`done` without
  the generation check: the waiter `_tick(gen)` checks it), M9 (the room rule's value condition: a reachable refetch loop was
  not found, kept as a defensive reserve during a resolution transition), M14 (the CriOS / EdgiOS / FxiOS marks: every iOS
  user agent carries iPad / iPhone / iPod as well).
- Found while testing: the gate's `done` nulled the waiter before reading it (fixed before the first run on the test site);
  a test that iterated a Map while `get()` re-inserted keys never ended (a harness slip, not the module).
