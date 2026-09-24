# Overlays — G5 adversarial review (coastline clip, client + coast data) — 2026-09-24

Gate G5 of plan section 19 ("Coastline boundaries"): the browser now clips the wave-height and
peak-period overlays to GSHHG coastlines (asset v2.6.1, `static_overlay/overlay.js`), using the
coast-v1 data built by `tools/coast/build_coast.py` and published under `static/coast/v1/` in the
frames bucket. Reviewed on `feat/overlays-coast` @ abd0791 (test site wave-app-clean.onrender.com,
asset 2.6.1) before the production merge. Three fresh-context reviewers at MAX effort, no access
to the author's reasoning:

- **A — client code** (Node harness, read-only): the coast-v1 decoder, tile geometry at world
  copies / the dateline / the poles, rasterisation and the nonzero union, `composeTile`, mask
  caching and invalidation, `readoutAt` = drawn pixel, lifecycle races (Off, field change, Update,
  chunk arrival, the 15 s watchdog), resource bounds (in-flight, LRU, masks), tests, security.
- **B — coast data and builder** (Python + the published bucket objects): GSHHG reading, ring
  normalisation (0..360 storage, Greenwich, the dateline, the polar ring), cell clipping and seams,
  quantisation, the coast-v1 encoding and `--check`, the published files decoded and spot-checked
  against known coordinates, the JS decoder vs the Python decoder, tier 0/1 consistency, workflow
  pins/secrets, bucket safety, LGPL compliance.
- **C — owner requirements on the test site** (the only reviewer with the browser): "extend to and
  stop exactly at all landmass coastlines" for the wave and period displays at zooms 5–11 around
  every Hawaiian island and in other regions, readout = drawn pixel, the tier switch, playback cost,
  section-7 regressions, phones, memory, console.

Severity: P0 = wrong picture/data reaching users, crash, hang, unbounded resource; P1 = owner
requirement not met, or a race/resource bound broken in realistic use; P2 = robustness,
performance, maintainability; P3 = nits. Every P0/P1 is fixed and re-reviewed before the merge.

## Findings

(filled in from the reviewer reports)
