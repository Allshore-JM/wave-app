# G67: adversarial review of the RRFS overlay client (plan section 99 D2)

Reviewed: feat/rrfs 430cd2c + the working tree's faster tile sampler and 0.1-screen region margin
(`static_overlay/overlay.js`, `app.py`, tests/overlay, tests/test_overlay_flag.py). Fresh-context reviewer, read-only,
probes in the playback harness, 27 mutants on a scratch copy. 2026-11-18.

Result: 0 P0, 1 P1, 2 P2, 6 P3; no GFS / ECMWF regression found. All fixed or recorded below; re-verified the same day.

Checked and correct: sampleRegions = regionsCode at every pixel (last column, the absent rule, region order, 180);
Alaska across the date line everywhere; the cover decoding; the validator (`__proto__` refused, keys under the prefix);
a decode outliving its abort is never cached or drawn; the run-string trap (a frame index never reused across models);
_syncRegions during playback; the dropdown gating; adoption by signature.

| # | Finding | Outcome |
|---|---|---|
| P1-1 | The region set went stale: Off, pan, On (the manifest reused within 30 min); RRFS -> an observation -> pan -> Wind; a pan while the first RRFS frame loads. The map then drew the old regions (e.g. Hawaii) over Anchorage until the next step or pan | FIXED: the set is reset in mount, unmount and _leaveObs and by every _syncRegions whatever the mode; a pan during the first landing re-asks the target under the mount's signal; a landing whose bundle was made for another set re-asks once. Tests: Off / pan / On, and a pan while loading |
| P2-2 | A region failing for now (a stall, a network error) was cached inside the step's bundle for ever: blank there, no retry, no outage count | FIXED: the region frame cools down (`unavailable['region:'...]`, not asked again meanwhile); the bundle is kept with a `retryAt` and asked again after it (only that region downloads; the others are decoded). A first fix that left such bundles uncached made the ring re-request them in a loop; caught by the scenario and replaced. Test |
| P2-3 | Every region failing with mixed kinds took the first region's kind: a 404 + a stall marked the step unavailable for good | FIXED: for good only when every failure is. Test |
| P3-4 | A stale "newer" banner never cleared when the pointer returned to the manifest in hand | FIXED: cleared when the signature matches. A withdrawn pointer (`complete: false`) is still ignored by the run check: RECORDED (the 6-h stale line shows; prune keeps the previous manifest's frames) |
| P3-5 | The banner said "A newer run (... 18Z)" for a regional manifest whose run repeats | FIXED: "Updated RRFS frames are available." |
| P3-6 | "outside RRFS" came from the regions' boxes: the North Pacific, interior Canada and box corners showed nothing | FIXED: from the coverage masks (data). Test: a point inside Alaska's box without data |
| P3-7 | Particles were born over the regions' boxes (mostly empty; doubled where boxes overlap) | FIXED: born in the coverage cells in view (the boxes past 4,000 cells). Test incl. both world copies at the date line |
| P3-8 | The decoded region frames were kept through Off and an observation's model switch | FIXED: cleared there |
| P3-9 | No in-flight sharing per region file between two bundles of one step (a zoom-out re-downloads) | RECORDED: rare (a set change mid-download); the browser cache serves the repeat |

Mutants (27; 18 killed first time). The survivors now have tests: M6 spawn without world copies, M7 a re-read pointer
with the same run and a new signature, M9 a manifest whose signature is not the pointer's, M22 no Mercator stretch on
the particles, M23 the last region winning the particles, M26 coverNear's southern row. Equivalent / harmless (frame
keys are immutable): M19 / M24 rcache kept or set after an abort, M21 the rcache key without the model, M15 the
setModel guard (mount leaves RRFS anyway).

Found by the performance gate in the same round (not the reviewer): at shark speed the full-resolution direction frames
were half the heap churn. The direction is now published at half resolution (every second node of each region grid,
as GFS's wdir; the job's `FRAME_RES`); the browser validates `grid_half` and builds the particles from the speed on the
full grid and the direction on the half grid. Local headless Chrome (own profile) at 1080p and the phone preset over
Oahu, the West Coast, Alaska and Seattle (two regions): 7.96-8.03 fps, gap p50 125-127 / p90 <= 136 ms, 0 skipped,
heap live <= +8.3 MB and peaks <= +49.3 MB over GFS at the same view (the section-50 rule: +25 / +50).
