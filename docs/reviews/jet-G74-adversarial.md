# G74: adversarial review of plan section 106's jet stream client (the "Jet stream" overlay behind JET_OVERLAY)

Date: 2026-11-22. Scope: `feat/jet` @ c5973b2 against production 462cb4c (which already carries the jet frames job, G74a):
`git diff origin/Live-Buoy-Update -- static_overlay templates app.py tests` (overlay asset 2.32.1). The client offers
"Jet stream" (250 hPa speed) for GFS and ECMWF only, behind `JET_OVERLAY=1`: ramp A over 50-200 kt, a pale teal shade under
50 kt (the owner's pick S2 from the look sheet), particles from `jdir` only in the cores, the wind's relief / outline /
resolution rules, and the 250 hPa height contours from `jhgt` through the isobar layer (24 / 12 / 6 dam, labels and readout
in dam, no H / L), with a gear checkbox "Height contours (jet stream)".

Two fresh-context reviewers, brief in the session's scratch (`jet106/g74_brief.md`: eight hard requirements):
- **A** (Opus 5.5, max effort): the code plus fuzzing in Node against the real module (composeTile, readout, contours,
  switch sequences, manifest variants, random operation sequences, parity with production's controller, 25 mutants).
- **B** (Opus 5.5, max effort): the live test site (wave-app-clean, 2.32.1) from headless Chrome with its own profile:
  every tile pixel against an independent decode, readout against the drawn pixel, contours against an independent trace,
  switches, teardown, failure injection, hosts contacted, phone, births of particles against the drawn value.

**Result: 0 P0, 1 P1, 2 P2, 12 P3** (distinct; A 0 / 0 / 2 / 10, B 0 / 1 / 0 / 4; B-P1-1 = A-2 rated P1; B-P3-1 = A-9,
B-P3-2 = A-8, B-P3-4 = A-7).

## What held up
- Flag unset: Wind speed, wave height, peak period, ECMWF wind pixel- and readout-identical to production (B: 2.31.2 vs
  2.32.1 on the live sites); `composeTile` byte-identical for every non-jet field over 4,000 random tiles (A); 200 random
  controller sequences identical to production's (A). Exception, by design: /notice/ecmwf names the 250 hPa fields (A-8).
- Colours: every tile pixel at z4 (2.82 M px) and z8 (2.94 M px) = an independent decode and colour rule; alpha exactly 204
  under 50 kt, 255 at or above (B).
- Readout = drawn pixel: 0 mismatches over 3,600 synthetic and 2,800 real points (A), 549 half + 1,098 full resolution
  points and 70 phone long-presses (B); "mph · dam" / "km/h · dam".
- Contours: an independent smoothing + marching-squares trace gives the module's crossings exactly (A, real jhgt frames);
  B's own trace of the raw frame at the world view, z3, z4, z5.9, z6, z7; 24 / 12 / 6 dam where due, dam labels, no H / L;
  the checkbox removes them, persists across a reload, comes back.
- Switches: jet <-> wind keeps the time and swaps heights <-> isobars; GFS <-> ECMWF keeps the time; RRFS never offers the
  jet; ECMWF 12Z (no jet) says "no jet stream frames in this run yet"; Off leaves no canvas, timer or request (A: 12
  scenarios + 13,500 random operations; B on the site).
- Missing jdir / jhgt, no jet, 404 / 500 / garbage frames, odd field definitions: a clear message or a quiet degrade.
- A jet session contacts exactly the hosts a wind session does (B).
- Playback (author): jet at parity with wind, 1080p and phone, 8 fps, 0 skipped, live heap jet <= wind.

## Findings and what was done

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| B-P1-1 / A-2 | P1 | Particles born on pixels drawn under 50 kt at the cores' rims: the floor was applied at the half-grid direction nodes and the lattice flowed wherever they carried 25 % of the weight. B on the site: 0.8 % of births at z4 (<= 6 px out), 1.5 % at z8 (<= 43 px), 4.7 % at z9 NW of Kauai (p50 30 px, max 99 px), lowest 38 kt on a phone. | FIXED (2.32.3): `flowField` samples each lattice row with the layer's own sampler (`_codeRow` on its own frame, full or half: the tiles' and the readout's) and leaves the flow at 0 where the drawn value is under the legend floor. Tests: a half-grid rim (points the node rule alone would move), a full-resolution dip between two core nodes at z9 (every lattice point checked against `layer.valueAt`); both fail without the gate. Fix round 1 (2.32.2, never on the test site before this round) interpolated the node values instead: exact at half resolution only, replaced. |
| A-1 | P2 | Wind of a run without the jet, then Jet stream: the message stays after a run with the jet is published; Retry reloads the pinned old run for good (the G68 P2-1 class). | FIXED: the shown time is carried free, the pin released and the pointer re-read on that mount, so Retry adopts the newer run at the same time (with or without the half-hourly check). Test from A's repro. |
| A-3 | P3 | The particle floor was a hard-coded 50 kt (1 ulp off the manifest's) and would not follow a legend change. | FIXED: the floor is the layer's legend floor (`vectorNodes` and the lattice gate); `JET_CORE_MS` gone; test with a 60-kt legend. |
| A-4 | P3 | The readout has no direction although the brief / plan said "speed (+ direction)". | RECORDED: the same as Wind speed's readout (no direction); the plan's wording was wrong. |
| A-5 | P3 | Update onto a run without the jet said `layer "jet" is not in run ...`. | FIXED: the jet's own message. |
| A-6 | P3 | `_contourPrefChanged` drops the contour cache whenever contours are not wanted (production: only on "isobars off"); unreachable with the flag unset. | RECORDED: harmless (at most 5 x ~56 KB re-fetched on the way back to wind). |
| A-7 / B-P3-4 | P3 | The heights' layer fetched `terrain_half.png` (52 KB) and redrew for marks it never draws. | FIXED: the terrain is asked only for a spec with H / L marks, once per layer (also when the jet's layer later serves the wind). |
| A-8 / B-P3-2 | P3 | /notice/ecmwf changes even with the flag unset. | RECORDED: accurate (the jet frames are public since STOP 1); the STOP 2 summary says so instead of "every page byte-identical". |
| A-9 / B-P3-1 | P3 | A portrait phone's widest view (zoom 1.7-1.8, ~162 deg) drew 12-dam lines, a quarter of the gaps under 8 px; the isobars take their widest interval at zoom <= 2.9. | FIXED: 24 dam to zoom 2.9 as well; tested. |
| A-10 | P3 | 5 of 25 mutants survived (no layer-level test of a drawn jet tile, of the heights' H / L / labels / line style, of the readout's unit). | FIXED: tests for a drawn jet tile (shade at 204, never opaque teal), the heights through the isobar layer on a recording canvas (dam pills every 12 dam, no H / L, the dark style), the readout's contour part via `_contourReadout`. |
| A-11 | P3 | A vacuous `|| true` assertion in the new playback test. | FIXED: removed. |
| A-12 | P3 | `test_overlay_module_unit_tests` failed once: reproduced only under load (4 parallel runs), in 7 pre-existing timing / memory tests, none from section 106. | RECORDED: not section 106; single runs pass. |
| B-P3-3 | P3 | The gear put the Particles slider between the two contour checkboxes. | FIXED: "Height contours (jet stream)" sits right under "Pressure isobars (wind)". |

Not proven (B): one transient `net::ERR_INSUFFICIENT_RESOURCES` on two jdir / jhgt frames, not reproduced in a second run or
60 s of playback; a real ECMWF run with the jet was not yet published (a simulated one drew and kept the time).

## Verification after the fixes
Fix round 1 (`a1eeb11`, 2.32.2: reviewer A's findings) and round 2 (`5706515`, 2.32.3: B-P1-1 exact, B-P3-3), test site
`test` @ caef9cd, 2.32.3 live 2026-11-22 02:53 UTC.
- Suites: Node overlay 789 / 789, UI 642 / 642, pytest 3,125 pass, 12 skipped (the two `tests/bathy` modules that import
  scipy do not collect on the author's machine: not installed here, unrelated).
- B's own birth measurement (`births2.js`: every live birth for 15 s, the drawn value under it from the site's own
  `readoutAt`), same views as B, on 2.32.3: Kauai z9 0 of 10,031 births under 50 kt (was 4.7 %), Oahu z8 0 of 11,536
  (was 1.5 %), North Pacific z4 0 of 6,209 (was 0.8 %), 40 N 175 W z6 0 of 11,423 (was 0.27 %), phone North Pacific z4 1 of
  1,327 (48.5 kt, 1 px from the coloured edge: within one lattice step, the bilinear position inside a cell whose corners
  all flow; was 1.1 %, down to 38 kt).
- Playback on 2.32.3 (the extra lattice sampling): 1080p North Pacific jet 8.00 / wind 7.97 fps, Oahu z8 jet 7.92, 8.01,
  8.02 / wind 8.02, 7.99, 6.82 (noise both ways), phone 8.03 / 8.03; 0 skipped; live heap after play jet <= wind every time
  (Oahu 44.6-44.7 vs 47.3-47.5 MB, North Pacific 40.8 vs 44.1), peaks within wind's spread (jet 86-98, wind 87-100 MB).
