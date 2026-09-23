# Overlays — G3 adversarial review (Phases 3 + 4: animation and all three layers)

Date: 2026-09-23. Scope: `feat/overlays-2` @ af415da (asset v2.3.0) on wave-app-clean.onrender.com with
`MODEL_OVERLAYS=1`; the frame job on `Live-Buoy-Update` @ bbfa2cd (manifest schema 3, live since the scheduled run
at 00:54 UTC published cycle 2026092218). Three fresh-context reviewers at max effort: **A** playback races and
resource bounds (Node harness with held decodes and a fake clock), **B** data semantics end to end (job diff, the live
bucket by HEAD/decode, field semantics, run switching), **C** owner requirements, §6 budgets and §7 regressions on the
test site in the Browser pane (desktop 1200×800 and the 375×812 phone preset).

**One P0 (B1, timing-dependent), five P1, eight P2, fourteen P3.** Everything P0–P2 in the client is fixed in asset
v2.4.0; P3s are fixed unless marked. Job-side items are collected at the end for a separate job-only push.

## Findings and disposition

| # | Sev | Rev | Finding (scenario → wrong outcome) | Fix (v2.4.0 unless noted) |
|---|---|---|---|---|
| 1 | **P0** | B (also A2a) | Cache, in-flight and unavailable keys were `res/field/step` with no run. A decode cannot be aborted once the body has arrived; one that finished after **Update** had cleared the cache put an old-run frame under the new run's key, and the next loop drew it under the new run's valid time (reproduced deterministically in Node). | Keys carry the run; the `_ensure` handlers neither cache nor deliver when the controller was aborted or the manifest changed (they reject as an abort); a late failure cannot mark the new run's frame; in-flight records are deleted only by their own fetch. Regression test `tests/overlay/playback.test.js` stages the race. |
| 2 | P1 | A1 | Nothing bounded fetches started by `seek`/`step`: dragging the timeline fired one fetch per slider value (41 concurrent, ≈ 213 MB of transient decode allocation); the owner bound is ≤ 2 in flight. | `_goto` trims the in-flight set to the new target's ring and keeps at most `MAX_INFLIGHT − 1` older fetches beside the target. Test: 12 rapid seeks never exceed 2 in flight. |
| 3 | P1 | A2 | Stale continuations: a decode that outlived a field change drew Hs data through the Tp palette under "Peak period"; one that outlived a resolution switch threw in `validateGrid` and killed playback with a spurious error. | `_goto` captures manifest/field/resolution and drops a landing that no longer matches; combined with #1. |
| 4 | P1 | C1 | Phones fetched **full**-resolution wave frames at the default zoom 6: an 81-frame Hs loop was 11.9 MB against the §6 phone budget of 5 MB (half loop 4.3 MB). | Narrow maps (< 700 px) stay on the half frames until zoom 7 for every field (dead band 7–7.5). |
| 5 | P1 | C2 | Heap saw-tooth: a fresh 256 KB `ImageData` per tile per frame plus a fresh 1440×721 decode canvas per frame (≈ 9 MB/s of garbage at 2 fps) pushed GC peaks to +36…+52 MB against the +25 MB budget (live set +12…+17 MB was fine). | One decode canvas for the module and one `ImageData` per tile canvas, reused (transparent pixels re-zeroed). |
| 6 | P1 | B2 / C4 | Wind loops: 33.6 MB full / 10.4 MB half (desktop budget 16 MB, phone 5 MB) — the §6 numbers were never revised after the full-field-over-land decision. | Wind stays on half frames until zoom 7 everywhere (desktop loop 9.9 MB). Owner decision remains on the table: restore full wind at zoom 6 (32 MB) or shrink the wind set (6-bit codes, WebP, 6-hourly phone stride). |
| 7 | P2 | A3 | Pause then Play (or hide/show) while a frame was loading started a second tick chain: 4 frames/s at 1×. | Play generations: a continuation from an older generation never schedules; `_after` clears a pending timer first. |
| 8 | P2 | A4 | An aborted tick load (zoom across the resolution threshold, or a field switch, while the target loaded) ended the chain with `playing` still true — frozen playback. | An abort during a tick moves on like a skipped frame. |
| 9 | P2 | C3 | Bucket outage mid-session: no error state, one "unavailable" entry per tick for 40 s, playback claimed to run; recovery only after the 60 s cooldown. | Three consecutive transient failures → "Overlay unavailable: frames cannot be loaded right now" with Retry (which clears the marks); playback pauses. |
| 10 | P2 | B3 | 403 was treated as permanent, but r2.dev answers 403 for bot-signature bans (every Python-UA HEAD got Cloudflare error 1010) while absent keys are 404. | Only 404/410 and decode failures are permanent. G2 #2 (custom domain before G4) stays open. |
| 11 | P2 | B4 | Scheduled ticks are dropped by GitHub: 22:37, 23:07, 23:37 and 00:07 never ran; the 18Z cycle was published 7.1 h after its run time although NOAA finished it at 22:49 UTC. | Job-side (below): denser cron and a lag figure in the run summary. |
| 12 | P2 | A5 (P3 there) | A missing first frame (404 on the frame `pickFrame` chose) failed the mount and Retry hit the same frame. | The mount falls to the next available frame once. |
| 13 | P3 | A6 | `nextAvailable` could return the current index when every other frame was unavailable: playing claimed but frozen, rebuilding a 633-character list per tick. | Never the current index; playback pauses when nothing else is left; the list is capped at 8 entries. |
| 14 | P3 | A7 | Prefetch tested raw truthiness, so a frame whose cooldown had expired was never prefetched (a stutter). | Uses the cooldown-aware check. |
| 15 | P3 | A8 | A run announced by the banner was not adopted on Off→On within 30 min; a field change while On could adopt it silently and lose the time position. | Adopted on the next mount after Off; a field change keeps the pinned run and the banner; a run change keeps the time position by nearest valid time. |
| 16 | P3 | A9 | In-flight records were deleted unconditionally by both handlers (latent). | Deleted only by their own fetch. |
| 17 | P3 | A10 | `resize` rebuilt the whole panel per event; a failed stylesheet retry waited 3 s each time; a script that evaluated but did not define the module left `loading` resolved. | Rebuild only when the panel moves between control and sheet; `loadAssets` rejects (and resets) when the module is undefined after `onload`. The stylesheet retry wait is accepted. |
| 18 | P3 | B5 | The stale banner was evaluated only at render; `_checkRun` did not validate `published_utc`. | The 30-min check validates the stamp and re-renders when the stale state flips. |
| 19 | P3 | B7 | Schema-3 template placeholders and frame monotonicity were not validated. | `validateManifest` requires `{res}`, `{field}`, `{step:03d}` and strictly increasing steps and valid times. |
| 20 | P3 | B8 | "≤ 0.0 ft" printed for Hs/wind code 1 (ice/enclosed water). | The ≤ prefix only where values below the floor exist (Tp). |
| 21 | P3 | C5 | With the flag on and the layer Off the map already had an eighth pane (`modelPane`). | The template no longer creates it; the module creates it on the first selection. |
| 22 | P3 | C6 | US↔Metric left the readout in the old unit until the next pointer move. | `refresh()` hides the readout. |
| 23 | P3 | C7 | Phone attribution wraps to two lines above the sheet (cosmetic). | Accepted. |
| 24 | P3 | C8 | A synthetic zero-duration touch fling drove the site's own `clampMapLatitude` into a `moveend` recursion (pre-existing, not overlay code; realistic drags are fine). | Noted for the site; not changed here. |
| 25 | P3 | B6 | Pre-v1 layout objects (`gfswave/0p25/2026092212/**`, `gfswave/0p25/latest.json`) are orphaned outside the pruned prefix. | Job-side (below). |
| 26 | P3 | B9 | A run listed as complete but persistently failing on GET exits 3 every tick with no failure record. | Job-side (below). |
| 27 | P3 | B10 | The bucket root is derived by stripping the literal v1 prefix; a `--force` on the live run rewrites immutable keys. | Operator notes; a `v2` layout needs a coordinated client change. |

## Verified OK by the reviewers (evidence in their reports)
- Single-run invariants: the valid-time line and collapsed title always describe the drawn frame; the thumb sits at the
  pending target with "loading +NN h…"; no violation in plain playback, seek storms, step spam, 404/503 skipping and
  resolution switches (A, invariant hook on `setFrame`).
- Bounds in playback: in-flight ≤ 2, cache ≤ 5 with the drawn frame protected, one tick timer + one 30-min interval,
  cadence 498–526 ms at 1× and 130–147 ms at 4× when prefetched, second loop 0 bytes (immutable cache) (A, C).
- Teardown: after Off — 0 timers, 0 map/document/container listeners, cache 0, in-flight 0, attribution and CSS
  variables cleared, panel and readout gone; Off during every loading stage and 20 Off/Hs/Tp/Wind/Off cycles in 5 s
  return to baseline; JS heap 3.8 MB after GC vs 6.4 MB baseline (A, C).
- §7 items 1–8 and 10–14 PASS on the test site; item 9 was the P2 above (C). Dateline continuity across world copies,
  marker taps during playback (forecast submit, live panel), pinch/drag through the overlay, `#tz` rule, hidden tab,
  reduced motion (no autoplay anywhere), newer-run banner and Update (C).
- Job: pointer written last, sidecar before manifest, partial builds never pointed to, prune protects the newest four
  complete runs and the pointed run; live bucket: 486/486 frame keys present with the sidecar's byte counts, immutable,
  CORS allow-list correct, `half == full[::2, ::2]`, land mask and value formula verified by decoding PNGs, retention
  correct; client `frameKey` ≡ job `frame_key` for all 486 (B). 32 job tests, 18 (+4 playback) Node tests, 23
  overlay/cache pytest, full suite green.
- Owner decisions confirmed on the site: opacity 0.65 / speed 1× defaults, layer Off on every load, opacity/speed
  remembered in the session, legend ranges and units, valid time in the table's zone with the run in UTC, attribution
  while On, the "not extra detail" caption, wind over land present (C).

## Re-verification on v2.4.0 (test site, 2026-09-23 ~04:00 UTC)
- Desktop 1200×800: keys carry the run (`2026092218/full/hs/9`); the map has 7 panes with the layer Off (the model pane
  appears on the first selection); 1× cadence 500–517 ms, cache ≤ 5, in-flight ≤ 2; a 30-seek storm leaves 1 fetch in
  flight; pause/play/pause/play while loading keeps one chain (10 frames in 5 s at 1×, min gap 482 ms); a blocked bucket
  reaches "Overlay unavailable: frames cannot be loaded right now" with Retry after 3 transient failures and playback is
  paused; Retry recovers without a reload (0 marks left); wind at zoom 6 is half-res (720 cols); Off leaves 0 tiles, an
  empty cache, no fetches and no timers. Console: only the pane's own blocked Cloudflare beacon.
- Phone 375×812: Hs at zoom 6 now half-res (720 cols, loop ≈ 4.3 MB); playback trough 15 MB / peak 32 MB from an 11 MB
  baseline; collapsed sheet carries its own pause button.
- Heap on desktop: trough +12 MB (live set) but GC peaks reached 82 MB during 8 s of playback — the remaining garbage is the
  4.15 MB `getImageData` copy per decoded frame plus the bitmap; Phase 5 replaces the canvas decode with a direct PNG
  inflate (`DecompressionStream`) that produces the 1 MB code array without the RGBA expansion.
- Budgets: desktop full loops Hs 11.9 MB, Tp 12.5 MB, wind 9.9 MB (half until zoom 7; 32.1 MB full above 7.5); phone loops
  at the default zoom Hs 4.3 MB, Tp ≈ 4.6 MB, wind 9.9 MB — wind still 2× the 5 MB phone line (owner decision, #6).

## Job-side follow-ups (separate job-only push, owner go-ahead required)
1. Cron every 10 minutes (a short-circuit run costs ≈ 20 s; public-repo minutes are free) and `published_utc − run_utc`
   in the step summary (B4).
2. Prune the pre-v1 prefix once (B6).
3. Count NotReady per run once the listing said complete; record a failure after N consecutive (B9).
