# G69 adversarial review: RRFS Precipitation total (section 102 B1, asset 2.30.0)

Reviewed on branch `feat/rrfs` (worktree `C:\Users\joshm\wave-app-rrfs`):

- `git diff 0fe1fe5 feat/rrfs -- static_overlay app.py templates tests`, minus the rrfs_wind / RRFS-table changes that came in with the production merge 3ad795a;
- the B1 commits e2a32fb, 4b216b3, a2726cc and b825d02.

The files are `static_overlay/overlay.js` (PrecipTotals and its wiring), `app.py` (RRFS_PRECIP_TOTAL), `templates/index.html` (PTOT), `tests/overlay/rrfs_ptot.test.js`, `tests/overlay/playback.test.js`, `tests/overlay/bootstrap.test.js` and `tests/test_overlay_flag.py`.

Line numbers are those of `overlay.js` at b825d02. The parent's legend merge arrived while this review ran (3fb61e3: PRECIP_TICKS / PTOT_TICKS, the shortened total-legend note, PTOT_TICKS exported). It moved only lines after 10457, by one, and left the engine unchanged.

## Test runs

- `node --test --test-timeout=20000 tests/overlay/rrfs_ptot.test.js tests/overlay/rrfs_precip.test.js tests/overlay/playback.test.js`:
  - 212 / 212 passed at b825d02;
  - 214 / 214 passed at 3fb61e3 (the parent's legend merge, committed during this review);
  - during the merge, before the US ticks were fixed, the parent's new tick test failed (`US: 0.25 / 0.5 collide`). The mutation run's baseline is that state; see below.
- Full node list `tests/overlay/*.test.js tests/ui/*.test.js`: 1 397 / 1 397 passed, exit 0.
- `python -I -m pytest -q -p no:cacheprovider tests/test_overlay_flag.py`: 24 passed, exit 0, at 3fb61e3.
  - A first run during the parent's uncommitted merge failed only `test_overlay_module_unit_tests` (the node suite run at that moment, which included the tick test); it was green once rerun after the commit.
- Scratch harnesses (not committed; under the session scratchpad `g69/`). They load the real `overlay.js` the way the tests do and drive `PrecipTotals` with a fake `ov`: async downloads with random delays, aborts, transient failures, and an rcache capped at 20 like `MAX_DECODED * 4`.
  - `fuzz.js` runs, per seed, 40 interleaved `at()` calls on 3 regions. The calls mix random idx / region sets / k0 / manifest (two signatures with different data), random aborts and 3 % failures. Every delivered bundle is checked against a direct sum of its own (manifest, k0, idx), and a fresh final call is checked too.
  - `wrap.js` / `wrap2.js` count downloads for the overlay's real ring order (`ringPlan`).
  - `demos.js` holds the scenarios quoted below.

## What holds (checked, no finding)

- **Integer exactness.**
  - `PRECIP_MILLI` is an Int32 per code: dry 0; codes 0 and 252-255 are -1, which counts as missing.
  - `sumApply` adds or subtracts whole integers, so adding an hour and taking it away again is exact. An Int32 sum cannot overflow (84 h x 75 000 = 6.3 M), and a Uint16 `bad` count cannot either.
  - Fuzz in the overlay's own abort discipline (`trim` mode: every k0 / manifest change first aborts every outstanding ctrl, as `_trimInflight`, `_prefetch` and `abortAll` do): 150 seeds, 2 746 delivered bundles, **0 wrong**, plus every post-run fresh call exact. This covers forward / back / restart, aborts at random points, failures, new manifest, new k0, and regions joining and leaving.
- **Order and aborts.**
  - Each region's hours are applied strictly in order, so `at` is always a prefix.
  - An abort or failure keeps the prefix. Results that arrive ahead of their turn wait in `got`; one left after a failure is dropped.
  - All workers settle before the queue moves on, so nothing adds after the next computation starts.
  - Mutants M2, M4, M14 and M25 (below) are caught by the fuzz.
- **Missing data is blank, not smaller.** A missing node in any summed hour (code 0, a 252-255 code, or a step whose region has no precip key: `q === null`) makes `bad > 0`, and `sumCodes` writes 0 (nothing drawn).
  - The job writes 255 where the hour's amount is unknown, and a run's f000 has no precip key. A step at or after k0 is never an f000, because f000 is valid at the cycle time, which is past once the source is published.
- **Code scale.** `ptotCode`'s binary search equals the rounded log formula (tested over 20 000 values). `ptotLut` has 512 entries; the largest index is `round(255 * 2) = 510`; index 2 is the dry tint. The sampler with `PTOT_TAB` (`K[c] = c - 1`, one type) never blends wet with dry.
- **Readout is the drawn pixel.** `readoutAt` / `valueAt` pass `this._tab` to the same sampler as `tileCodes`.
  - A node with a missing hour gives `v = null`. `_regionWord` then returns null inside a region (readout hidden) and "the total starts at the hour now in progress" on a before frame.
- **Units.** `ptotReadout` uses `precipAmountText(mm, unit, '')` (US: 3 / 2 / 1 decimals, Metric: 1 decimal under 10 mm). The legend positions US ticks at `v * 25.4` mm on the same `ptotPos` axis as the LUT, so the bar colour at a tick is the pixel colour for that total (alpha aside).
- **Flag off.**
  - `PTOT = PRECIP && flag`; the option and `FIELDS.ptot` exist only then (bootstrap + flag tests).
  - With the flag off nothing calls the engine: `_key` adds `/k` only for `field === 'ptot'`, the lineTimer branch needs `field === 'ptot'`, and `_ensureRegional` routes to the engine only for ptot.
  - `fields.ptot` is added to every precip manifest, but no code iterates `m.fields` for regional manifests (the only loop, 663, is the schema-2 GFS validator).
  - Wind keeps `sampleRegions`. Hourly Precipitation keeps `PRECIP_TAB` (`setFrame` sets `_tab` on every frame, which kills M17), and its keys, LUT and readout are unchanged. The rcache is shared by file key, so a precip -> total switch reuses decoded hours.
- **Rollover in the overlay.** `_key` carries `/k<k0>`. At the first call after the hour, every caller (`_goto` -> `_trimInflight`, `_prefetch`) rebuilds its rank with the new k0, so in practice all `/k<old>` computations are aborted before `at()` resets the engine. The lineTimer re-asks the shown step when idle and re-renders the panel (new legend start). Playback picks up the new key on its next tick. k0 == n gives a before bundle on every frame and the legend says "Total from now".
- **Memory.** The engine is bounded: per region an Int32 sum and a Uint16 count; `made` holds 8 bundles; regions only accumulate up to the 4 RRFS regions; everything is dropped on a new signature or k0. A long session does not grow (see P3-7 for what Off keeps).

## Findings

### P0

None.

### P1

None. No path found that draws a wrong total in normal use. The only wrong-total path found (P2-3) needs an event within about a millisecond of the hour boundary.

### P2

**P2-1. The ring wraps past the last frame into frames 0, 1, ...; when one of those is at or after k0 the engine "starts again" and throws away the running sum, so the next ring entry re-adds the whole run.**
Where: `ringPlan` 5692 (modulo n), `_prefetch` 8897, and the restart rule in `_compute` 428-431.

Scenario A, paused (the natural "total for the whole run" action): seek to the last frame (83 of 84) while k0 = 1. k0 = 1 is common right after a new manifest: frame 0 is the newest hourly cycle's +1 h, and that cycle publishes 1-2 h after its cycle time.
- The paused ring is [83, 0, 1, 82, 81].
- 0 is a before bundle. 1 is >= k0, so `st.at - idx = 82 > idx - k0 + 1 = 1` and the region restarts: sums zeroed, at = 0.
- Then 82 is "forward from 1": 81 adds.
- `wrap2.js` (rcache 20): **165 hour downloads instead of 83** for one seek.

Scenario B, playing at 4x (ring 6 ahead, 1 behind), k0 = 3, steps 70..83:
- per-step downloads are `74 1 1 1 1 1 1 1 0 0 0 78 79 82`;
- each of the last three steps re-downloads the whole run, because the wrap entries 3..5 restart the sum and the "behind" entry re-adds about 78 hours.
- Each such prefetch also holds the serialized queue until `_trimInflight` aborts it.

The output stays exact; the cost is bandwidth and CPU, about 80 region frames per step near the end of every lap and twice the downloads for the most common seek.

Fix (either or both):
- for `ptot`, do not wrap the ring (`ringPlan` with no modulo past n-1) and skip ring entries below k0;
- in the engine, never let a request whose answer is a cheap restart destroy a far sum: compute such small totals in a scratch sum instead of restarting `st`, or keep `st` and restart only for the target (not prefetch).

Add a test: seek to n-1 with k0 = 1 through the playback harness and count fetches.

**P2-2. One region-hour that fails for good (404 / decode) takes every region's total from that hour on, and every later step asks for the dead file again; a stalling hour reaches the outage panel after three steps.**
Where: `_compute` worker 451-460 (any rejection sets `failed`, which fails the whole call), `_hour` 481-487 (it never reads or writes `unavailable['region:' + rk]`), and `_ensureRegional` 8738-8745.

Scenario (`demos.js` a): region b's hour 12 answers 404. Totals for [a, b] at 12, 13 and 20 all reject with "frame 404", and the dead file is requested again for each step (3 times here, once per step in playback). Region a alone at 20 is fine.

In the overlay, `_ensureRegional` marks each step unavailable for good. Playback (`nextAvailable`) skips them, so from that hour on the whole total (every region) is never drawn, while hourly Precipitation shows every other region.

With a transient stall instead (3 tries per `_download`), each step fails transiently; after `MAX_TRANSIENT = 3` the step's error gets `outage` and the panel shows "frames cannot be loaded right now", for one region's one hour.

Fix:
- In `_hour`, treat a **permanent** failure (failureKind) of a region-hour as "no frame" for that region: resolve `null`, which marks every node of that region missing so it draws blank from that hour on. Record `unavailable['region:' + rk] = true` so it is not asked again, and honour that mark (and a live cooldown) before downloading.
- Keep a transient failure fatal for the call, so it is retried later.

Test: 404 on one region's hour; the other region's total is drawn and the dead URL is fetched once.

**P2-3. `made` is keyed by `idx|names` only; a computation that is not aborted when the engine resets for a new k0 or manifest stores its old-k0 bundle into the new `made`, and later calls for the new k0 return it.**
Where: `at` 407-417, `_keep` 474-478, and `_ensureRegional` 8715 (k0 read a second time from `Date.now()`).

`reset()` replaces `made`, `regions` and `queue` while the old computation keeps running. On success it calls `self._keep(key, bundle{k0: old})` on the new map, and `at(m, idx, names, newK0)` then hits it.

Engine-level reproduction (`fuzz.js raw`, where old ctrls are not aborted on a change): 150 seeds, **20 wrong bundles**, the first a k0 = 3 call answered with a k0 = 2 bundle. With the overlay's abort discipline (`trim`) it is 0. The overlay is protected only by timing:
- `_trimInflight` / `_prefetch` build their ranks with one `Date.now()`;
- `_key` and `at()` each read it again;
- if the hour turns between the rank read and the `at()` read, the `/k<old>` ring computations survive the trim (their keys were in rank), the new `at()` resets the engine, and when they finish their old-k0 bundles land in the new `made`;
- the next request for that idx with the new k0 draws a total that includes the hour now over, under a `/k<new>` key, and the lineTimer will not notice (`totals.k0` is already new).

The window is tiny (well under a millisecond of a synchronous `_prefetch` / `_goto` per hour boundary), but it is silent, persistent until eviction, and the fix is one line.

Fix:
- In `_compute`'s success path, refuse to store or deliver a stale result: `if (self.sig !== sig0 || self.k0 !== k0) throw abortError();` before `_keep`, with `sig0` captured at the start of `_compute`. Mutant M30 (`_keep` only when `b.k0 === this.k0`) keeps the suite green.
- Also have `at()` check `hit.k0 === k0`.
- In `_ensureRegional`, compute k0 once and use it for both the key and `at()`.

**P2-4. The engine discards the running sum where one hour would do: at every hour rollover, and when a queued request is aborted before it runs.**
Where: `at` 408 (reset on any k0 change), and `_compute` 428-431 (the restart branch zeroes `st` at planning time, before any download, even when `ctrl` is already aborted).

- **Rollover** (`demos.js` c): on frame 80, k0 2 -> 3 costs **78 downloads**; taking hour 2 away would cost 1, and the integers make that exact.
  - This happens every hour while the panel is open, including in a hidden tab: the lineTimer has no `document.hidden` check, and background tabs still run a 60-s interval.
  - With the rcache at 20 entries, every hour re-downloads about 80 region frames per region in view.
- **Aborted queued request** (`demos.js` b): sums at 80; a request for idx 3 is queued and aborted before it runs (a timeline drag through the early frames, each seek aborting the previous one); then 79 is asked.
  - The aborted request still zeroes the sums (restart chosen at planning time). 79 then costs **78 downloads** instead of 1.
  - The result is exact, only wasteful.

Fix:
- On a k0 change within the same signature, keep the regions and subtract hours `k0old .. k0new-1` from each region whose `at >= k0new - 1`. Otherwise restart that region only.
- Skip the lineTimer rollover while `document.hidden` (do it on `visibilitychange`).
- In the queued run, check `ctrl.signal.aborted` before `_compute`, and apply a restart only when its first hour is added (or plan without mutating).

**P2-5. Tests miss engine rules that matter for correctness, and the controller paths of B1.** 9 non-equivalent mutants survive the three test files. Three of them give WRONG totals, caught only by this review's fuzz harness:

- **M23** (`idx < k0` -> `idx <= k0`): the first hour's total, the hour now in progress, is drawn blank. No test asserts a bundle at `idx === k0`.
- **M19** (the queued run's stale check removed): a request queued under the old manifest / k0 runs against the reset engine with the old manifest's frames and corrupts the new sums. Fuzz: 65 wrong bundles in 60 seeds.
- **M4** (hours added in arrival order instead of strictly in order): exact only when every hour lands. Once an abort or failure follows an out-of-order arrival, `at` and the sum disagree (fuzz: 99 wrong). The existing out-of-order test never combines out-of-order arrival with an abort or failure.

At controller level nothing exercises:
- the hour rollover (lineTimer, M11);
- the legend start hour (M31: the start shown an hour late survives);
- the readout text through `_bindReadout` (M18);
- the US legend tick positions (M21: US ticks placed as if they were mm survive);
- the made cap (M7: an unbounded `made` survives);
- the restart rule's cost (M24);
- the ring wrap (P2-1) and a failing region-hour (P2-2).

Fix:
- add engine tests for `at(k0)` (the first hour is wet), for a queued request after a reset (M19), and for out-of-order arrival followed by an abort (M4);
- add playback-harness tests that move `Date.now` across an hour boundary with a frame on the map (expect a new `/k`, a re-summed bundle and the legend's new start hour), and that read the readout over a wet pixel in US and Metric;
- check `buildTotalLegend` tick `left` values for both units, and `made.size <= PTOT_KEEP`;
- add the 404 region-hour and wrap tests of P2-1 and P2-2.

### P3

**P3-1. The "Adding hours: n of N" note disappears for the rest of a long sum after a rollover or manifest change.** When the old (aborted) computation settles, its error handler sets `self.progress = null` and calls `_totalProgress`. The new computation's workers only update `if (self.progress)`, so they never show it again (`demos.js` d: "progress hidden (new run still going)"). Fix: keep `progress` per computation (a token) and clear it only if it is still yours.

**P3-2. The legend's start hour is read from `Date.now()` at render time, not from the drawn bundle.**
- `_totalStartText` (10482) and the drawn total can disagree for up to a minute after the hour: any render between the boundary and the lineTimer's re-sum (unit change, panel toggle, a menu) shows the new start over the old picture.
- While playing across the hour the panel is not re-rendered: the picture uses the new k0 and the legend keeps the old start until something renders.

Fix: build the text from `layer._frame.k0` (it is in every bundle) and re-render the line when a bundle with another k0 lands.

**P3-3. A failed re-sum at rollover leaves the old total and no retry.** The lineTimer calls `_goto(frameIndex)` and swallows failures, but `at()` has already set `totals.k0` to the new hour. Later ticks see `totals.k0 === precipK0(...)` and never retry, so the map keeps the total that includes the hour now over (with its old legend, so it is at least labelled) until the viewer moves. Fix: compare against the drawn bundle's k0 (`layer._frame.k0`) instead of `totals.k0`.

**P3-4. Caps are not said.**
- A sum past 1000 mm reads "1000 mm total" / "39.4 in total", where 84 h of RRFS can reach 6 300 mm.
- An hour at the top hourly level (every amount from about 70 mm/h up) counts as 75 mm and is not flagged.
- Hours under 0.1 mm are dry (code 1) and count 0: 84 h of 0.09 mm/h drizzle reads "no precipitation" against about 7.6 mm.

Fix: "≥ " at level 253; optionally carry a "capped" bit per node (`bad`-like counter of top-level hours) to prefix "≥"; mention in the legend that hourly amounts under 0.1 mm are not counted.

**P3-5. Blank areas have no word.** A node blank because of a missing hour, or a region whose step has no precip key, hides the readout (`nofield: 0` in every total bundle; `_regionWord` only words hourly Precipitation). Hourly Precipitation says "no precipitation frame for this hour". Fix: count the regions with any `bad` node (or `q === null` hours) into the bundle and word it, for example "an hour of this total is missing".

**P3-6. Landing on a frame before k0 shows an empty map.**
- A reload restore (`restoreIndex` to the saved frame), a model carry (`nearestIndex`), a hourly-Precipitation -> total switch on a past frame, or an hour rollover while on k0 can all land before k0.
- Before-frame bundles draw nothing, and only a hover says why.

Fix: on mount / restore / field switch clamp `idx` to `max(idx, k0)` for ptot; consider stepping the slider to k0 at a rollover that would leave it before k0.

**P3-7. Off and other fields keep the totals.** `unmount` clears the rcache but not `this.totals`. A CONUS total keeps a 3.7 MB sum, a 1.9 MB count and up to 8 bundles of 0.9 MB (about 13 MB; Alaska similar) after the viewer switches to Wind or Off. Fix: `this.totals = null` (or `reset`) in `unmount` and when the field leaves `ptot`.

**P3-8. The engine assumes the frames are consecutive hours.** Each hourly frame is a one-hour amount and the sum adds frame by frame. A manifest with a gap (allowed by the validator, which needs only increasing times) would undercount silently instead of blanking. The job's merge is contiguous today (RUN_STEPS 0-84, CYCLE_STEPS 1-18). Fix: in `_compute`, treat `valid[k] - valid[k-1] !== 3.6e6` as a missing hour (`q = null`) for every region.

**P3-9. The legend no longer says the total is water (3fb61e3, the parent's legend merge).** The shortened legend note ("Total from X to the slider time.") dropped "snow and ice as water". Snow, mix and ice hours are summed as liquid water, so a reader in a snow area may take "1.2 in total" for snow depth (and the readout has no type word either). The US tick collision seen while that merge was in progress is fixed in the commit (US ticks 0.01, 0.1, 0.25, 1, 2, 10; the new test passes). Fix: keep "(snow as water)" in the note or in the readout ("1.2 in total, as water" where any summed hour was snow, mix or ice).

**P3-10. Comment drift.** `_totalStartText`'s comment says "the hour ending 4 PM"; the code returns the hour's start (`valid - 1 h`), which is what the legend text needs. `PTOT_KEEP` is described as "the ring around the slider", but the 4x play ring is 8 entries, equal to the cap, and the wrap entries of P2-1 push real ring entries out.

## Mutation testing

Each mutant was one exact string replacement (applied only when the original occurred once) in a **copy** of `overlay.js` in a scratch tree (`g69/tree`, with copies of `tests/overlay` and `tests/fixtures`). The worktree was never edited: the parent was mid-merge.

For each mutant two things ran:
- `node --test --test-timeout=20000` on rrfs_ptot, rrfs_precip and playback. "Killed" means more failures than the baseline; the baseline has the parent's one failing legend-tick test.
- The `fuzz.js trim 60` exactness harness against the same copy.

The copy was restored from memory after each mutant.

| # | Mutant | Tests | Fuzz (wrong bundles / 60 seeds) | Verdict |
|---|---|---|---|---|
| M1 | a missing hour is never taken away (`bad += 1` on the way back) | killed (rrfs_ptot x2) | 215 | killed |
| M2 | a back step leaves `at = k` | killed (playback B1) | 196 | killed |
| M3 | back-or-restart tie: `<=` -> `<` | survived | 0 | SURVIVED, equivalent (same cost at the tie; both exact) |
| M4 | hours applied as they arrive, no strict order | survived | 99 | SURVIVED, not equivalent (wrong after an abort / failure; P2-5) |
| M5 | no reset on a new k0 | killed (rrfs_ptot) | 8 | killed |
| M6 | queue bypassed (computations in parallel) | killed (playback B1) | 1009 | killed |
| M7 | `made` never evicts | survived | 0 | SURVIVED, not equivalent (unbounded memory) |
| M8 | k0 with `>=` | killed (rrfs_ptot) | 0 | killed |
| M9 | total code one level low | killed (rrfs_ptot) | 0 | killed |
| M10 | bundle key without `/k<k0>` | killed (playback B1) | 0 | killed |
| M11 | lineTimer rollover check removed | survived | 0 | SURVIVED, not equivalent (the total never moves its start; P2-5) |
| M12 | `_hour` ignores the rcache | killed (rrfs_ptot, download count) | 0 | killed |
| M13 | an abort after the last hour still delivers | survived | 0 | SURVIVED, equivalent (the bundle is exact; `_ensureRegional` drops aborted results anyway) |
| M14 | a late hour after a failure is still added | survived | 0 | SURVIVED, equivalent (still an in-order prefix: exact) |
| M15 | missing hours ignored in the codes (smaller total) | killed (rrfs_ptot x5) | 1028 | killed |
| M16 | dry counted as missing | killed (rrfs_ptot x6) | 953 | killed |
| M17 | totals sampled with the hourly table | killed (playback B1) | 0 | killed |
| M18 | the readout's `total` branch removed | survived | 0 | SURVIVED, not equivalent (the readout reads the code through the generic unit path) |
| M19 | queued run's stale (sig, k0) check removed | survived | 65 | SURVIVED, not equivalent (old-manifest frames summed into the new sums; P2-5) |
| M20 | "before" readout word removed | killed (playback B1) | 0 | killed |
| M21 | legend US ticks positioned as mm | survived | 0 | SURVIVED, not equivalent (US ticks a factor 25.4 off) |
| M22 | an hour without a frame (q null) ignored | killed (rrfs_ptot x2) | 335 | killed |
| M23 | `idx < k0` -> `idx <= k0` (the first hour blank) | survived | 62 | SURVIVED, not equivalent (the hour in progress shows nothing; P2-5) |
| M24 | restart never chosen going back | survived | 0 | SURVIVED, correctness-equivalent, not cost-equivalent (no test counts a long back move's downloads) |
| M25 | restart without clearing the sums | killed (rrfs_ptot, playback B1) | 423 | killed |
| M26 | the ptot LUT branch removed | killed (playback B1) | 0 | killed |
| M27 | ptot mounts without `fields.precip` | killed (playback B1) | 0 | killed |
| M28 | `PTOT_CONC = 1` | killed (rrfs_ptot progress count) | 0 | killed |
| M29 | progress never shown | killed (rrfs_ptot) | 0 | killed |
| M30 | (not a mutant: the P2-3 fix) `_keep` only when `b.k0 === this.k0` | green | 0 | the fix keeps every test green |
| M31 | legend start hour = the frame's valid time (an hour late) | survived | 0 | SURVIVED, not equivalent (P2-5) |

Totals: 30 mutants (M30 is the fix probe), 18 killed by the tests, 12 survived.
- 3 survivors are equivalent (M3, M13, M14).
- 1 is correctness-equivalent but not cost-equivalent (M24).
- 8 are real gaps (M4, M7, M11, M18, M19, M21, M23, M31). The fuzz harness catches M4, M19 and M23, which the suite misses.

After the run the scratch copy was restored, and `git status --short` in the worktree shows only this report (untracked). The parent's merge commit 3fb61e3 was made by the parent, not by this review.

## Summary

| Severity | Count | Items |
|---|---|---|
| P0 | 0 | - |
| P1 | 0 | - |
| P2 | 5 | P2-1 ring wraps past the end and restarts the sum (165 downloads for one seek to the last frame; about 80 per playback step near the end); P2-2 one dead region-hour fails every region's total from that hour (and is re-asked per step; a stall reaches the outage panel); P2-3 `made` keyed without k0/signature: a non-aborted computation across a reset stores a stale-k0 total (guarded only by timing); P2-4 rollover and aborted queued requests throw away the running sum (78 downloads where 1 would do, hourly, even in a hidden tab); P2-5 tests miss engine rules (M23 first hour blank, M19 stale queued run, M4 order under abort survive) and the controller paths (rollover, readout text, legend hour and ticks) |
| P3 | 10 | progress note lost; legend start from the clock, not the bundle; failed rollover never retried; caps unsaid (1000 mm, top hourly level, sub-0.1 mm); blank areas without a word; landing before k0 shows nothing; totals kept after Off; consecutive-hours assumption; the water-equivalent note gone from the legend (3fb61e3); comment drift |

## Fix round (2026-11-19, feat/rrfs)

Every finding is fixed or accepted with a reason:

| Finding | Outcome |
|---|---|
| P2-1 ring wrap wipes the sum | Fixed twice over. For ptot the ring no longer wraps past the last frame and skips frames before k0 (`_plan`). In the engine each region keeps up to two windows (PTOT_STATES = 2), and a restart reuses the window with the lower end, so the run-long sum survives a look at an early hour. Test: "an early hour is summed in a second window". |
| P2-2 one dead region-hour kills every region | Fixed. `_hour` treats a permanent failure (404 / decode) as a missing hour for that region only (null: blank from that hour on), marks it `unavailable['region:'+rk] = true` and never asks again. An active transient mark rejects without a download. Test: 404 on b's hour 12. |
| P2-3 stale bundle stored after a reset | Fixed three ways. `_compute` throws AbortError when (sig, k0) moved on before it ends; `_keep` refuses a bundle of another (sig, k0); `at()` checks the hit's k0 and sig. `_ensureRegional` takes k0 from the key itself (one clock read). Test: "a total finished after its start hour moved on". |
| P2-4 sum discarded at rollover / by an aborted queued request | Fixed. Windows cover from..at; a new k0 moves the front (one hour taken away per region) instead of summing again; the queued run checks `ctrl.signal.aborted` before planning; the lineTimer skips a hidden tab. Test: "a new start hour moves the window front". |
| P2-5 test gaps | Fixed. Engine tests for M23 (the first hour), M19 (queued under an old manifest), M4 (out of order, then abort), M7 (made cap), gap, 404, rollover cost; a controller test crossing the hour (start + 1 h, legend start follows, five hours not six, at most 2 fetches) with readout words in both units and the mount landing on a summed hour; a panel test for M21 (US tick positions). M18 (the readout handler line itself) is covered only through `ptotReadout` on the drawn code, since the fake frames are wet at a corner node where the pixel sampler is blank; accepted. |
| P3-1 progress hidden after a reset | Fixed: progress carries a token; only its own computation clears it. |
| P3-2 legend start from the clock | Fixed: `_totalStartText` uses the drawn total's k0 (same signature) and records `_legendK0`; the lineTimer re-renders once when a drawn total's k0 differs from the legend's. |
| P3-3 failed rollover re-sum never retried | Fixed: the lineTimer compares the drawn total's k0, not `totals.k0`. |
| P3-4 caps not said | Partly fixed: the top total level reads "≥ 982 mm total" (its lower edge, as the hourly readout does). Not said: hourly amounts under 0.1 mm count as dry, and the hourly top level counts as 75 mm. Accepted: the owner asked for fewer notes under the legend (2026-11-19). |
| P3-5 blank areas have no word | Fixed: each total part carries `missing`; `_regionWord` says "an hour of this total is missing here". |
| P3-6 landing before k0 | Fixed for mount / restore / switch: `idx = max(idx, min(k0, n - 1))` for ptot. Rollover while on k0 still shows the "starts at the hour now in progress" word; accepted. |
| P3-7 totals kept after Off | Fixed: `unmount` and any mount of another field drop `this.totals`. |
| P3-8 non-consecutive frames | Fixed: a frame whose valid time is not one hour after the previous one counts as a missing hour. |
| P3-9 "as water" dropped | Accepted: the owner asked for the hourly note to go and for minimal legend text; the hourly legend's type bars still say "Snow (as water)". |
| P3-10 comment drift | Fixed (the start-text and PTOT_KEEP comments). |

Suites after the round: node 1,409 / 1,409 (exit 0); `tests/test_overlay_flag.py` 24 passed.
