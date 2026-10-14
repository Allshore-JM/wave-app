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

## Test-site re-measure of the fix round (step 5b)
2026-10-14 (Opus 5.5, MAX). 2.16.4: `test` @ 2f61fdb (cherry-picks of e4f0ba1 + aecbe35; README merged cleanly), deployed
03:43:52 UTC. 2.16.5 (the owner's decision below): `test` @ f4762e9 (cherry-pick of 8cac824), deployed 06:54:03 UTC. Each
time the served overlay.js / overlay.css = the feature branch byte for byte and the previous version's URLs answer 404.
Headless Edge with the GPU (Intel Iris Xe: canvas / compositing / raster enabled). The acceptance script = reviewer B's w54
(page + worker heap) plus the painted rule (`unpainted N/M`: a landing followed by the next before any animation-frame
callback ran). Production and test back to back after >= 155-s idles, the order alternated; runs 2026101318, 2026101400
and 2026101406, each warmed for both Origins before use (every cell an edge HIT except the one noted). Logs: scratch
`wind/verify5/out/s5b.log` and `w54_*.json` (table: `wind/verify5/table5b2.py`), `b01_identity.txt`, `b01p_decoder.txt`,
`b07_stuckzoom_*.txt`, `b19_nullctx.txt`, `b04_longhide.txt`, `b23_workermem.txt`, `b24_4k.txt`.

Functional drills (2.16.4; the decode functions are the same in 2.16.5):
- Picture identity (B's b01): worker vs `?ovdecode=main`, wave height full + pdir half and wind half + wdir half at steps
  0 / 57 / 120 / 384: every code array identical, 192 of 192 tiles' composed pixels identical. The rewritten decoder against
  PRODUCTION's (2.14.12) on the same run and frames: 16 of 16 code arrays identical (b01p).
- Stuck zoom (b07, Chromium and the Safari UA): zoomstart + movestart with no end, playing or paused, and a lone movestart:
  healed within 1-2 s with the canvas VISIBLE after the heal (G29 B-P2-1 fixed); a real zoom afterwards normal; no console
  problem.
- Refused 2D context (b19, Safari UA, playing at 4x, the fresh canvas's getContext stubbed to null at a zoom): the old canvas
  kept (ctx present), playback on, state ready, the readout present, 0 console problems (G29 B-P2-2 fixed; 2.16.3 went to
  "Overlay unavailable").
- 330 s hidden while playing (b04): healed 0, the first frame 89 ms after the page was shown, the overdue check after it.

Timed cells (fps, gap p50/p90 ms, unpainted / landings, render p50 = the median gap between animation frames; heap = page
+ worker isolates, live after GC and peak, MB above the Off baseline). Q = the quiet machine (background CPU 8-23 %);
O / O2 = a ~25 % CPU load proxy (3 busy threads; the owner's usual apps were not running); G = a GPU-bound diagnostic at
3840x2160; M / M2 = 2.16.5 (background 0-38 %). The acceptance lines: shark gap p50 <= 130 and p90 <= 150, fish p50 <= 256,
snail p50 <= 502, 0 skipped, 0 unpainted.

| phase | run | cell | production 2.14.12 | test | live prod -> test (diff) | peak prod -> test |
|---|---|---|---|---|---|---|
| Q | 1318 | shark-1080-fresh | 7.4 fps, 133/140, 0/207, 55 ms | 2.16.4 worker: 8.0 fps, 124/141, 0/224, 43 ms | 26.3 -> 32.0 (+5.7) | 49.0 -> 53.2 |
| Q | 1318 | shark-1080-pan | 7.7 fps, 129/141, 0/216, 53 ms | 2.16.4 worker: 8.0 fps, 125/136, 0/225, 41 ms | 27.6 -> 33.4 (+5.8) | 60.6 -> 49.2 |
| Q | 1318 | shark-1280-fresh | 7.7 fps, 130/137, 0/215, 25 ms | 2.16.4 worker: 8.0 fps, 125/135, 0/225, 22 ms | 22.5 -> 27.6 (+5.1) | 38.6 -> 42.4 |
| Q | 1318 | shark-1280-pan | 7.6 fps, 131/139, 0/214, 24 ms | 2.16.4 worker: 7.9 fps, 125/135, 0/224, 22 ms | 22.9 -> 28.7 (+5.8) | 39.1 -> 40.8 |
| Q | 1318 | fish-1080-fresh | 3.9 fps, 254/266, 0/110, 50 ms | 2.16.4 worker: 4.0 fps, 250/261, 0/112, 46 ms | 26.2 -> 29.3 (+3.1) | 41.7 -> 43.2 |
| Q | 1318 | snail-1080-fresh | 2.0 fps, 506/514, 0/55, 46 ms | 2.16.4 worker: 2.0 fps, 500/511, 0/56, 44 ms | 26.2 -> 26.7 (+0.5) | 38.1 -> 46.8 |
| Q | 1318 | wind-shark-1080 | 7.6 fps, 132/139, 0/213, 53 ms | 2.16.4 worker: 8.0 fps, 125/135, 0/225, 40 ms | 18.7 -> 20.2 (+1.5) | 46.2 -> 42.7 |
| Q | 1318 | phone-shark | 7.5 fps, 133/136, 0/135, 17 ms | 2.16.4 worker: 8.0 fps, 124/135, 0/144, 17 ms | 10.4 -> 12.9 (+2.5) | 20.3 -> 22.8 |
| Q | 1318 | shark-1080-noanim | 7.5 fps, 133/139, 0/136, 17 ms | 2.16.4 worker: 8.0 fps, 126/138, 0/144, 17 ms | 22.2 -> 26.4 (+4.2) | 56.9 -> 42.3 |
| O | 1318 | shark-1080-fresh | 7.6 fps, 128/146, 0/213, 64 ms | 2.16.4 worker: 7.9 fps, 126/144, 0/223, 39 ms | 26.8 -> 34.2 (+7.4) | 43.1 -> 46.8 |
| O | 1318 | shark-1080-pan | 7.7 fps, 127/147, 0/215, 83 ms | 2.16.4 worker: 8.0 fps, 125/145, 0/224, 47 ms | 27.6 -> 34.2 (+6.6) | 45.9 -> 51.6 |
| O | 1318 | wind-shark-1080 | 7.4 fps, 132/149, 0/208, 65 ms | 2.16.4 worker: 7.9 fps, 127/142, 0/223, 41 ms | 17.5 -> 20.2 (+2.7) | 41.9 -> 54.9 |
| O | 1318 | fish-1080-fresh | 3.9 fps, 257/275, 0/110, 57 ms | 2.16.4 worker: 4.0 fps, 249/265, 0/112, 50 ms | 26.3 -> 29.4 (+3.1) | 36.8 -> 41.1 |
| O | 1400 | shark-1280-fresh (test: the run was new and cold) | 7.6 fps, 131/147, 0/215, 26 ms | 2.16.4 worker: 7.0 fps, 128/243, 0/196, 24 ms | 21.9 -> 27.6 (+5.7) | 33.5 -> 43.2 |
| O2 | 1400 | shark-1280-fresh | 7.7 fps, 130/144, 0/214, 24 ms | 2.16.4 worker: 8.0 fps, 126/137, 0/225, 21 ms | 21.9 -> 30.6 (+8.7) | 34.4 -> 39.7 |
| G | 1400 | shark-2160-gpu (pair 1) | 3.6 fps, 280/293, 0/100, 12 ms | 2.16.4 worker: 4.0 fps, 252/270, 0/112, 236 ms | 59.7 -> 69.3 (+9.6) | 111.7 -> 121.0 |
| G | 1400 | shark-2160-gpu (pair 2) | 3.6 fps, 271/282, 0/103, 12 ms | 2.16.4 worker: 4.1 fps, 245/262, 0/115, 229 ms | 61.0 -> 68.3 (+7.3) | 96.2 -> 122.9 |
| M | 1400 | shark-1080-fresh | 7.6 fps, 132/139, 0/214, 53 ms | 2.16.5 main: 8.0 fps, 124/134, 0/225, 38 ms | 26.3 -> 31.7 (+5.4) | 46.3 -> 46.9 |
| M | 1400 | shark-1080-pan | 7.6 fps, 131/139, 0/215, 51 ms | 2.16.5 main: 8.0 fps, 125/135, 0/223, 39 ms | 27.7 -> 33.3 (+5.6) | 41.9 -> 47.1 |
| M | 1406 | shark-1280-fresh | 7.6 fps, 130/139, 0/213, 27 ms | 2.16.5 main: 8.0 fps, 126/136, 0/225, 22 ms | 21.8 -> 27.3 (+5.5) | 27.6 -> 37.6 |
| M | 1406 | shark-1280-pan | 7.6 fps, 130/139, 0/215, 25 ms | 2.16.5 main: 7.9 fps, 125/134, 0/221, 22 ms | 23.7 -> 28.3 (+4.6) | 31.1 -> 37.3 |
| M | 1406 | fish-1080-fresh | 3.9 fps, 255/266, 0/110, 49 ms | 2.16.5 main: 4.0 fps, 250/257, 0/112, 46 ms | 26.3 -> 28.9 (+2.6) | 51.6 -> 60.8 |
| M | 1406 | snail-1080-fresh | 2.0 fps, 507/517, 0/55, 47 ms | 2.16.5 main: 2.0 fps, 502/510, 0/56, 45 ms | 26.3 -> 26.5 (+0.2) | 48.7 -> 49.3 |
| M | 1406 | wind-shark-1080 | 7.6 fps, 131/141, 0/215, 59 ms | 2.16.5 main: 8.0 fps, 125/138, 0/224, 42 ms | 17.6 -> 19.9 (+2.3) | 43.5 -> 34.5 |
| M | 1406 | phone-shark | 7.5 fps, 133/137, 0/137, 17 ms | 2.16.5 main: 8.0 fps, 124/133, 0/145, 17 ms | 10.3 -> 12.8 (+2.5) | 18.8 -> 22.2 |
| M2 | 1406 | shark-1080-fresh | 7.4 fps, 132/149, 0/208, 55 ms | 2.16.5 main: 8.0 fps, 126/136, 0/224, 41 ms | 26.4 -> 31.7 (+5.3) | 58.8 -> 48.0 |

Results on 2.16.4: every acceptance cell PASSES in both machine states: shark 1080p fresh 8.0 / 7.9 fps and pan 8.0 / 8.0
(gap p90 136-145), 1280 fresh 8.0 and pan 7.9, fish 4.0, snail 2.0, wind 8.0 / 7.9, phone 8.0. 0 skipped and 0 UNPAINTED
landings in every cell on both sites; fetches per frame 1.00-1.02; in flight never above the cap. Production in the same
conditions: 7.4-7.7 fps (gap p50 127-133: its cadence drifts), 0 unpainted. The one failing test cell (O shark-1280-fresh,
7.0 fps, p90 243) met run 2026101400 two minutes after it published: 403 of 432 frames MISS at the edge (p50 252 ms; K-2);
re-run warm (O2): 8.0 fps, 126/137. At 4K (G) the GPU renders only every ~230 ms with test's particles, and test lands one
frame per render (4.0-4.1 fps, 0 of 227 unpainted: the paint gate; 2.16.3 replaced frames unseen exactly here); production
3.6 fps. The particle loop at 4K: production 9.8, test 7.3 particle frames/s (look D's strokes, GPU-bound). 4K is outside
the owner's 1920x1080 target; its second pair's peak (122.9 MB with the worker) is 1.2 MB over production's highest + 10.
The ~25 % CPU proxy did NOT reproduce the slower state reviewer B measured (both sites kept their quiet numbers under it), so
B's state was not CPU-bound (GPU or thermal, K-4); the paint gate covers it by construction (one landing per render).

Memory on 2.16.4 (owner decision 3, page + worker heap, same cell): test's live set +0.5..+8.7 MB above production's (max: O2
shark-1280-fresh +8.7); peaks within production's range for the cell + 10 MB in every cell up to 1920x1080; the decode
worker's own heap now 1.2-2.5 MB on average (G29: 33-37 MB, a sawtooth to ~65). The operating system's view (renderer
private bytes during 1080p shark, a FRESH browser per measurement, alternating order) shows what the heap metric cannot:
production 192-196 MB mean / 200-206 max; test 213-222 / 223-234; test with `?ovdecode=main` (no worker) 200-202 / 208-211.
So the page itself costs ~+7 MB over production (the bigger ring at 4x) and the decode WORKER a fixed ~17-20 MB, also while
paused (its thread and isolate; its V8 heap is only 2.4-5.5 MB committed). Worker vs main-thread decode at 1080p shark
(ABBA): quiet, main 7.9 / 8.0 fps (125/135, 125/133), worker 8.0 / 8.0 (125/136, 126/137); under the ~25 % load, main 8.0 /
7.9 (125/143, 126/144), worker 8.0 / 8.0 (125/143, 126/144); 0 unpainted in all eight. The worker saves 6-15 ms of
main-thread time per frame (tasks 614-618 vs 663-667 ms per second quiet, 708-710 vs 786-833 loaded) and buys no frame rate.

**Owner's decision (2026-10-14): decode on the MAIN THREAD by default; the worker only with `?ovdecode=worker` (a
diagnostic).** Asset 2.16.5 @ 8cac824: `FrameDecoder.available()` is false unless the page's address carries
`?ovdecode=worker` (the old `?ovdecode=main` now means the default); README; Node overlay 243 + flag tests 11; both mutants
of the switch killed.

2.16.5 on the test site (M / M2 rows): every acceptance cell PASSES. Shark 1080p fresh 8.0 fps (124-126 / 134-136) on two
runs and pan 8.0 (125/135); 1280 fresh 8.0 (126/136) and pan 7.9 (125/134); fish 4.0 (250/257); snail 2.0 (502/510, on the
502 line); wind 8.0 (125/138); phone 8.0 (124/133). 0 skipped and 0 unpainted in every cell (1,655 of 1,655 landings by the
painted rule); fetches per frame 1.00-1.02; in flight never above the cap. Production in the same pairs: 7.4-7.6 fps at
shark, fish 3.9, snail 2.0 (gap p50 507, over the line), phone 7.5, 0 unpainted. Memory rule (same cell, same metric): test's
live set +0.2..+5.6 MB above production's; every peak within production's measured range for the cell + 10 MB (the closest:
fish 60.8 against production's 36.8-51.6, i.e. 9.2 above its highest; phone 22.2 against 18.8-20.3; snail 49.3 against
38.1-48.7; shark 1280 fresh 37.6 against 27.6-38.6). OS view (fresh browser, 1080p shark): production 187-196 MB mean /
199-206 max (paused 177-186); main-thread decode (2.16.5, and 2.16.4 with `?ovdecode=main`) 200-203 / 208-211 (paused
190-202.5), plus one start-of-playback sample at 223.7 (the first of its series; the rest <= 210). That is +8..+16 MB above
production pair by pair (+9 on average; +5 on the heap metric), down from +17..+30 with the worker.

Outcome of the step-5b items:
- B-P1-1: FIXED. 0 unpainted of 6,229 test landings at 1920x1080 and below (2.16.4 with the worker 3,680, with `?ovdecode=main`
  894, 2.16.5 1,655) and 0 of 227 at 4K, where the GPU renders every ~230 ms; production 0 of 4,461.
- B-P1-2: FIXED. The worker keeps nothing (its heap 1.2-2.5 MB on average) and since 2.16.5 there is no worker by default;
  the memory rule holds in every cell up to 1920x1080 on both versions.
- B-P1-3: the line is reached on this laptop quiet and under a ~25 % CPU load (8.0 / 7.9 fps at 1080p shark on both decode
  paths); reviewer B's slower state was not CPU-bound and was not reproduced. Where the browser renders slower than the
  cadence, the paint gate lands one frame per render (4K, above), never a frame unseen. Owner awareness: on a hotter or busier
  GPU the 1080p rate can fall below 8 fps; no frame is then skipped or replaced unseen.
- The drills (B-P2-1, B-P2-2, B-P3-2) behave as fixed; the iPad remains the owner's check with `?ovdebug=1`.
Next: step 5c, a short fresh re-check (the paint gate under a fake-rAF fuzz, the main-thread decoder and the worker switch,
the animator guards), then the owner's go-ahead for production (tag `prod-pre-fast` @ 908af0b, fast-forward).
## Re-check of the fix round (step 5c)
2026-10-14 (Opus 5.5, MAX): one fresh-context reviewer on `git diff 583455a..8cac824` and the test site (2.16.5). Report:
scratch `g29/recheck/recheck-report.md`, with its scripts and logs beside it. The reviewer ran:
- a fake-rAF fuzz of the paint gate (reviewer A's world plus a render model, landing hooks and gate invariants, three
  decode modes; 1,300 runs, 87,000 operations, 29,374 playback landings without reduced-motion toggles);
- hand checks of the cadence under fast, slow and absent rendering;
- the decoder against Node's real DecompressionStream;
- the busy rule's gesture scenarios, 29 real user agents and 54 mutants;
- live drills on the test site (headless Edge, GPU).
The author re-ran R-1's scenario on overlay.js @ 8cac824 and read every finding against the code.

**0 P0, 0 P1, 3 P2, 3 P3.**

| # | Sev | Finding | Outcome |
|---|---|---|---|
| R-1 | P2 | **A landing of an older play generation replaces the current paint gate and strands playback.** `_paintWait(gen)` (2493) has no generation check and starts with `_paintClear()`, which also drops the CURRENT generation's stored waiter; its own `done` then refuses that waiter (`gen !== playGen`). The old chain (pause + play, or hide + show, while the playhead waits for a frame) lands the same frame AFTER the new one when it is longer: in production when the old `_goto` waited for the frame's direction and the new one does not, i.e. reduced motion was switched on in between (on an iPad: Settings > Reduce Motion while the overlay plays, then back). Then nothing is pending while `playing` stays true, until the viewer presses pause and play. Reproduced in Node (late landing, slow render, hide + show), by the fuzz (4-5 stalls per 300 runs with the toggle, none without), and live on the test site on the first try. Author: re-ran `r1_stale_gate.js` on 8cac824: the three variants stall (frame 15, playing, no timer / waiter / animation frame), the control plays on; with the guard all four play on (frame 54 after 5 s). | FIX (scope 1) |
| R-4 | P2 | **The watchdog's "busy 10 s in one go" adds up separate gestures.** `_check` (1707) samples the busy flags every 2 s and resets `busySince` only when a check sees the map idle (or after a heal), so drags or pinches that each span a check add up, and a check that returns early (hidden page) leaves a stale `busySince` for the next gesture. False heals mid-gesture: 56 of 200 "active" and 190 of 200 "busy" 60-s gesture streams; live, real mouse drags took `flow.healed` 0 -> 2. A heal mid-pinch shows the canvas for the rest of the pinch, and `healed` / `heal:suspended` enter the `?ovdebug=1` readout the owner will read on the iPad. Author: code read; `resume` clamps the suspend counter at 0, so a false heal leaves no lasting state. | FIX (scope 2) |
| R-5 | P2 | **Regression with `?ovdebug=1`: in the error panel every `_syncUI()` throws.** The fix round's error branch (3270) sets `this.ui = { debug }`; `_syncUI` (3435) guards only `!ui` and writes `ui.title`. Off (`unmount` -> `pause` -> `_syncUI`) then stops half-way: the picture, the run timer and the animator stay and the tab's saved field stays, so a reload brings the overlay back; a desktop resize and a zoom across the resolution threshold throw too (the latter inside Leaflet's zoomend dispatch). Live: reproduced, and the guard patched into the page fixed it. Author: code read (the other users of `this.ui` guard their own members). | FIX (scope 3) |
| R-2 | P3 | A frame that lands after a pause arms the gate while paused (an animation frame and a 250-ms timer); harmless today (its `done` refuses the stale generation; play / pause / Off clear it), but it is what turns into R-1 when `play()` does not clear first (mutant G8). | FIX (R-1's guard) |
| R-3 | P3 | Seeks are not gated: a seek made while playing can be replaced by the next playback landing before it is painted (11 of 200 near seeks at shark in the model); production behaves the same. | ACCEPT: not reachable from the page. Every seek path pauses first (`step`, `_scrubTo`, the ribbon keys), and the page never calls `seek()` (author: code read) |
| R-6 | P3 | Suite gaps: 17 of 54 mutants survive the suites, and six of those describe real behaviour: G2 (`_paintWait` without its first clear) and G8 (`play()` without clearing) stall the fuzz; D4 (inflater errors keep their own message: a corrupt or truncated stream would be classed transient and retried every 60 s instead of permanent); D8 (no fallback for a refused transfer: a worker decode would never answer); R1 (the readout timer would not stop for a detached element); R2 (the error panel's readout: no harness loads the module with `?ovdebug=1`, which is how R-5 got through). | FIX: pins (scope 4) |

Confirmed by the reviewer (evidence in the report):
- **Paint gate.** The fuzz held its invariants in every run without the toggle: no unpainted landing, exactly one pending
  continuation while playing, nothing after Off, caps, the drawn frame kept by value, no frame of another run, none
  skipped. The detector caught three gate mutants in 25-33 of 40 runs. Cadence: renders every 16-33 ms give exactly 500 /
  250 / 125 ms; renders every 130-200 ms at 4x give one landing per render, no burst; with no animation frame the 250-ms
  fallback paces 4x at 250 ms.
- **Decoder.** The fixtures are byte-identical; random images with all five filters match with the IDAT split into 1-byte
  pieces, halves and empty pieces. Fourteen broken-stream kinds are refused as a permanent 'frame decode failed', and a
  256-MB decompression bomb is refused in 4 ms. An abort before or while the body arrives works. The 2.16.5 switch was
  checked in 15 address cases; live, the default boots no worker and `?ovdecode=worker` boots one. The worker's transfers
  and its refused-transfer fallback give an identical picture.
- **Animator, tails, user agents.** The animator guards hold (A1-A10 killed). Live hide / show: 0 unpainted of 139
  landings. Tails span 600-733 ms of flow at 15-60 Hz and x1-x4. `isWebKit()` is right for 29 real user agents.
- **Could not check:** the real iPad and Safari, Firefox, a 10-minute soak, and the worker's picture identity on 2.16.5
  (b01 covered it on 2.16.4; the decode functions are unchanged).

Fix round 5d scope (overlay asset 2.16.6):
1. R-1 + R-2: `_paintWait(gen)` returns unless playing and `gen` = `playGen`, so a landing of an older play, or one after
   a pause, arms nothing. `done` runs the waiter it holds (the waiter `_tick(gen)` checks its own generation), so even a
   stale gate cannot drop the current one. Tests: the reviewer's scenarios (late landing, slow render, hide + show, with
   the reduced-motion change); play right after a post-pause landing; nothing armed while paused.
2. R-4: the stuck clock restarts with every map event: `_note` (zoomstart / zoomend / movestart / moveend / viewreset /
   resize), plus 'move' and 'zoom', so a moving gesture never counts as stuck. A busy flag with no map event for 10 s still
   heals. Tests: separate drags spanning checks; a hidden period then a new drag; a 12-s moving drag (none of these heal);
   a flag stuck with no event, and a zoomstart without zoomend (these still heal).
3. R-5: `_syncUI` returns when there is no panel title. Test: the panel harness loaded with `?ovdebug=1`: the error state
   carries the readout, and `_syncUI` / `pause` do not throw.
4. R-6 pins: D4 (a corrupt and a truncated deflate stream give 'frame decode failed', classed permanent); D8 (a refused
   transfer sends the code array alone); R1 (the readout timer stops when its element is detached); G2 / G8 through item 1.
5. README (the watchdog sentence); asset 2.16.6 and the fixture; mutants of the fixes.
Then the test site (MAX): the reviewer's live drills (R-1 on a throttled network, R-4 with real drags, R-5 in the error
panel), the gate fuzz on the fixed code, and a 1080p shark pair against production. After that, the owner's go-ahead for
production.
## Fix round 5d (overlay asset 2.16.6)
Built 2026-10-14 (Opus 5.5, HIGH) on `feat/overlay-fast` after df874e9: `static_overlay/overlay.js`, the overlay Node suites
(`playback`, `flow`, `overlay`, `panel`), `app.py` (the version), the asset fixture, README. Every scope item of the re-check:
1. **R-1 / R-2**: `_paintWait(gen)` returns unless the overlay is playing and `gen` is the current `playGen`, so a landing of
   an older play, or one after a pause, arms nothing. The gate's `done` runs the waiter it holds; the waiter (a tick)
   checks its own generation. Tests: the reviewer's scenario as a test (late landing, slow render, hide + show, each with
   reduced motion switched on in between, and the control): playback goes on with something pending. Second test: a frame
   that lands after a pause arms no gate (no animation frame, no fallback timer), and play then lands the next frame and
   plays on.
2. **R-4**: every map event restarts the stuck clock: `_note` (zoomstart / zoomend / movestart / moveend / viewreset /
   resize) and new 'move' / 'zoom' listeners, so a moving drag or pinch never counts as stuck. A busy flag with no map event
   for 10 s still heals and shows the canvas. Test: separate drags spanning checks, a hidden period then a new drag, a
   moving drag, a moving pinch (none heal); a zoom flag stuck with no event (healed, canvas shown).
3. **R-5**: `_syncUI` returns when there is no panel title. The error panel's `?ovdebug=1` readout alone is no panel.
4. **R-6 pins**: D4 (a corrupt and a truncated deflate stream: 'frame decode failed', classed permanent); D8 (a refused
   transfer list: the code array goes alone); R1 / R2 and R-5 (the panel harness loaded with `?ovdebug=1`: the error panel
   carries the readout, `_syncUI` and `pause` do not throw there, and the readout's timer stops once its element is gone).
5. README (the watchdog: "busy for 10 s with no map event in between"), asset 2.16.6, the fixture.
- Suites: Node overlay 248 (was 243), pytest 1,068. Mutants of the fixes (scratch `fast/mut5.py`, the file restored from a
  byte copy): 9 of 10 killed. N2 (`done` keeps its old generation check) survives as the redundant half of the R-1 fix:
  undoing both halves (N3, the 2.16.5 gate) is killed.
- The reviewer's gate fuzz on the fixed code (`g29/recheck/out6/`): 1,000 runs, 70,000 operations, 1,870 reduced-motion
  toggles and 20,493 timed playback landings across the canvas, main-thread and worker modes: 0 failures. The fuzz counted
  618 calls of `_paintWait` from an older play generation; each is now turned away at the guard and arms nothing.
