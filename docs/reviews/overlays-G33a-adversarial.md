# G33a: adversarial review of the Great Lakes frames job and the lakes coast set (plan section 50)

Target: `feat/great-lakes` @ 4a7d19d (diff base production c81d6a0): `tools/model_frames/glwu.py`, `glwu_fetch.py`, the
GLWU additions to `decode.py`, the optional Source hooks in `run.py`, `encode_frame`'s `cells` / `resolutions`,
`publish.GLWU_PREFIX`, `model-glwu.yml`, the trigger / keepalive / tests wiring, `tools/coast/build_coast.py --lakes`.
One fresh-context reviewer at HIGH (2026-10-21), real data from GLWU run 2026102119 (96 paced NOMADS requests, no full
file), its own worktree, 46 mutants.

## Result
0 P0, 0 P1, 4 P2, 9 P3. Every data claim held up:
- Regrid: 0 mismatches at 16,000 random nodes against the reviewer's own great-circle-nearest search over all 209,741
  source cells (eccodes' coordinates); over all 315,192 inside nodes 10 near-ties (<= 0.09 m), 0 water/land changes;
  the Lambert maths matches eccodes to 2.7e-12 cells; the sphere (shapeOfTheEarth 6) is right.
- Meanings: PERPW = the bulletins' peak period (median difference -0.01 s); DIRPW FROM; WIND/WDIR = the earth-relative
  NDFD wind, FROM (the grid-rotated alternative is off by 3.8 deg median); WW3's own source leaves the period undefined
  on ice / inactive points, so "blank where no period" matches the model.
- Bulletins: the 72 ids = the directory minus the 24 rip points; all 72 pass the check; they keep the WW3 TO
  convention (the site's parser adds 180, as for GFS-Wave).
- C8: GFS and ECMWF builds byte-identical between c81d6a0 and 4a7d19d on the same input (2,946 objects).
- Pipeline: prune isolation, the pointer last, fill_guard with this source, the summary without a wind field.

## Findings and outcomes
| # | Finding | Outcome |
|---|---|---|
| P2-1 | The period (and direction) is drawn over flat-calm water: 7-55 % of the lakes under 0.1 m still carry a period, and the frame's longest period sat there (7.0 s vs 5.1-5.9 s over waves). Author's check, 13Z f000: 5,164 calm cells with a period, max 6.3 s vs 6.21 s over waves. | FIXED (owner 2026-10-21: "A"): below `CALM_M` 0.1 m the cell keeps its height but has no period or direction. |
| P2-2 | A NOMADS outage (5xx, timeout, reset) failed the workflow every tick (a mail every 10 min). | FIXED: an outage in discovery, in a step or a short read / HTTP 416 is not ready (exit 3, `notready/`, no failure record); the 18-h stale alarm reports a long outage. |
| P2-3 | The frames were hostage to the bulletins: > 7 bad, or one unreachable, failed the whole build. | FIXED: a missing, bad or unreachable bulletin is left out (after a throttle the rest are not asked); more than 7 is a summary WARNING; the site's table will use the newest kept run that has the bulletin (step 5). |
| P2-4 | `check_lakes` switched its probes and area check off when the lake count was wrong. | FIXED: a build from the pinned GSHHG 2.3.7 archive (its sha256) always gets the probes in both tiers, the area, and 3 lakes / >= 100 islands. |
| P3-1 | The .idx is not always written after the GRIB (one minute before; one re-posted). | FIXED: a short read or 416 is not ready. |
| P3-2 | "no complete run on S3 in the last 48 h" for a NOMADS 24-h source. | FIXED: the source names its host (`where`). |
| P3-3 | `--allow-partial` wrote the run's immutable winds.json and bulletins. | FIXED: a partial build stores neither. |
| P3-4 | Ice not observable (no winter run); the 2-node fill can colour ~3 km over an ice edge. | ACCEPTED, documented. |
| P3-5 | winds.json FROM vs bulletins TO undocumented. | FIXED: documented (glwu.py, README). |
| P3-6 | GLWU has no water at the Bay of Quinte, the Thousand Islands. | ACCEPTED (model domain), documented. |
| P3-7 | The tests workflow ignored fixture-only changes. | FIXED: `tests/fixtures/glwu/**`, `tests/fixtures/ecmwf/**` in its paths. |
| P3-8 | Test gaps: the decode glue (M29-31), the budget (M21, M27), the Retry-After cap and the ladder (M07, M12), the wind NaN guard (M24), the row count (M14), the zero-area ring (M43), the lakes real branch (M41, M42), constants (M06, M22, M23). | FIXED: tests for each (a fake eccodes module for the glue). |
| P3-9 | The GLWU tests took 74 s of the suite. | FIXED: the whole-job tests run on a coarse grid (the real grid is pinned by its own test): 29 tests in 9 s. |

## Fix round (verification)
- Mutants: the reviewer's real survivors + mutants of every fix: 29 / 29 killed (scratch g33a/mut2.py).
- Suites: tests/model_frames 283 passed; tests/coast 23 passed. GFS output identical before/after again (63 objects).
- Real dry run (run 2026102119, all 150 steps + bulletins): complete, 72/72 bulletins, 8.2 MB a run, station winds 0
  differences against an independent nearest-water search; the real lakes build passes the strengthened --check.
