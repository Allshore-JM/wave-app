# Overlays — G12 adversarial review (follow-ups after plan section 21) — 2026-09-26

The owner asked for all follow-up items after the particle animation went live (asset 2.9.2, production 7f92530):
the job's direction-mismatch warning (it fired for a 1 cm ice-edge cell), the build time (664 s for run 2026092518),
the half-resolution wave direction under the full-resolution field near coasts (G10-B P3-1), and the phone controls
below the fold of the sheet (G8 B-P3-4). Branch `feat/overlays-followups`.

- Job @ 3b80b42: `_nearest_values` works on the target cells only (bit-identical to the whole-grid version; 0.63 s →
  0.06 s per field); `build_and_publish` is a pipeline (a prefetch of the next step's records, a 5-thread encode pool, an
  8-thread upload pool with a bounded backlog; GRIB decoding on the main thread); `pdir_mask_mismatch` counts waves of at
  least 0.1 m without a direction.
- Client @ e8cf753 (asset 2.9.3): the wave direction switches to the 0.25-degree frames once the map is zoomed in from the
  site's default zoom 6 (from 6.5); on phones the Opacity / Contours / Animation row sits under the timeline and the
  header line carries the valid time.

One fresh-context reviewer at high effort (Opus 5.5).

## Findings: 1 P0, 0 P1, 2 P2, 7 P3

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P0-1 | P0 | `drain()` asked each upload future `done()` twice (once to pick the ones to check, once to drop the finished ones): a failed PUT that finished between the two questions was dropped unchecked, and `drain(0)` had the same gap, so the stats, the manifest and `latest.json` could be written over a missing frame with no failure record. Deterministic repro with the real `build_and_publish`: the pointer flipped to a complete manifest with 727 of 729 PNGs; with real threads 4,661 of 10,633 injected failures were never raised. | Fixed @ 8999f01: each future is asked once and then checked or kept. Regression test with a future that reports "not done" once and "done with an error" afterwards, found by a later drain and by the final `drain(0)`; it fails on the old code. |
| P2-1 | P2 | Phones lost the "loading +N h…" / "unavailable" hint: the settings row pushed the Valid line below the fold of the 66-px details box. | Fixed @ d1ccf62 (asset 2.9.4): the sheet's header line shows a pending seek ("+213 h → +9 h loading…"). |
| P2-2 | P2 | The new header clipped the forecast hour at the end of the line (every title at 360 px, every non-US time zone at 375 px). | Fixed @ d1ccf62: the header leads with the hour, then the valid time; the field's name (shown by the select above the map) is left out on phones. |
| P3-1 | P3 | At 320 px the settings row wrapped and Animation fell below the box. | Fixed @ d410aaa (asset 2.9.5): a tighter row on screens ≤ 340 px; verified on the test site for all three fields. |
| P3-2 | P3 | The zoom buttons step 0.5, so 6 → 6.5 → 6 kept the full-resolution direction at the default zoom (hysteresis back below 6); the README sizes held only on a fresh load and were slightly low. | Back to half below 6.25; README sizes from the live stats of run 2026092518 (20.5 / 32.8 MB desktop, 11.6 / 24.0 MB phones, wind 22.9 MB). |
| P3-3 | P3 | The job's 0.1 m cut is on model values, the client's floor on decoded ones (from 0.0886 m); the period layer draws particles on cells under 0.1 m (floor tp ≥ 3 s). | Accepted: the stat monitors the model grids; no difference on the real f024 grids. |
| P3-4 | P3 | No test for the new phone state (half-resolution field under the full-resolution direction); a stale test name. | Tests added (vectorNodes and the scheduler state); name fixed. |
| P3-5 | P3 | A failed build waits at exit for the running prefetch and PUTs (bounded, ~260 s worst case). | Accepted. |
| P3-6 | P3 | Screen readers: the header and the Valid line both update every frame inside the panel's `aria-live` region. | Accepted for now (the Valid line did so before). |
| P3-7 | P3 | Small touch targets (13-px checkboxes, a 60-px slider). | Accepted: each checkbox's label text is part of its tap target. |

## Verified correct by the reviewer

`_nearest_values` bit-identical to the 7f92530 version on 3,000 random grids (poles, the dateline, edges, ties, float32
and float64, every window size), 800 `fill_coast` runs and the error path; on the real f024 grids every field's full and
half PNG bytes and stats match. eccodes only on the main thread; the fill mask loaded before the encode threads; encode
threads touch only their own arrays; the boto3 client is thread-safe. An upload failure exits 1 with a failure record;
"not ready" from the prefetch still exits 3; the dry run creates no upload pool; memory bounded. Encode 2.03 → 0.83 s
per step on 4 pinned cores; estimated 3–4 minutes per run on the runner (was 664 s), single-threaded JPEG2000 decoding
now the bottleneck. Node 113/113 and pytest 71 before the fixes; the playback harness move from zoom 7 to 6 weakened no
test. Phone sheet at 375×812 and 360×640 for all three fields: the settings row fully visible, the sheet 38.8 % of the
map and clear of the zoom/Home column, real taps work; the desktop panel unchanged. Not verifiable: real runner
timings, JPEG2000 decode speed, iOS/Android font widths, the true production odds of the P0.

## After the fixes

Node 115, pytest 394, flag tests 10. Test site on asset 2.9.5: the header "+129 h · Sep 30, 05:00 PM HST", a pending
seek "+9 h → +129 h loading…", the settings row on one line at 44–61 px in the 66-px box at 360 and 320 px.
