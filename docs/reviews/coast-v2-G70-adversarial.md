# G70: adversarial review of coast set v2 (plan section 103, stage S1 Hawaii)

**Reviewer:** one fresh reviewer, 2026-11-19. It read the code and the plan, ran the suites, and ran scripted geometry
checks on a real build.

**Scope:** `feat/coast-v2` @ 21b0c62 plus the working-tree pin, against production 2eac9fc:
- the builder `tools/coast/build_coast_v2.py`;
- the `coast-build.yml` set `coast-v2`;
- the hazard lines (`tools/alerts/build_alert_coast.py --coast v2`, `alert_coast_v2.json`);
- the COAST_V2 switch (`app.py`, `templates/index.html`, `static_overlay/overlay.js`);
- the tests.

**Brief:** find real defects before an immutable upload (a year of caches) and a production flag switch. Read-only.
The review covered:
- builder correctness;
- the check and upload gates;
- the workflow;
- the hazard lines;
- the server/client switch;
- client decoding of the v2 cells.

**Result:** 0 P0, 2 P1, 3 P2 (one an owner-awareness item), 6 P3. No defect in the geometry, the seam at cell and box
lines, the byte-identity proof of the unchanged cells, or the client switch.

## Findings and what was done

| # | Finding | Fix |
|---|---|---|
| P1-1 | `alert_coast_v2.json` was built from a superseded v2 build (lon/lat clip), and no test tied the hazard file to the coast it follows. | Rebuilt from the pinned build. New test: `alert_coast_v2.json["coast"]["v2"] == build_coast_v2.PIN_TIER1_SHA256`. |
| P1-2 | Nothing checked was the bytes an upload publishes: the Windows build differed from the runner's, and the pin was uncommitted. | P2-1's fix made the builds identical: dry run #711 on the runner gives the laptop's hash `9a0ca75f…`, now pinned. The step-6 spot checks and the hazard lines were redone on those bytes. |
| P2-1 | "Exact on any platform" was false; the cause was unknown. | `shapely.normalize` on the unioned, speck-filtered geometry before Douglas-Peucker, which is anchored at each ring's start. Runner and laptop now agree (#711). Docstrings corrected. |
| P2-2 | No test covered the upload gates. | `test_upload_refuses_every_unproven_build` covers five cases with `BC.upload` stubbed (detailed below the table). |
| P2-3 | Owner awareness: Maui Nui strips up to ~0.9 km wide change from GSHHG land to water. | DEM depths there are 5-20 m. Imagery checked on the local harness: v2 matches the shore at Molokai north, Maui north (Paia) and Lanai north. HIZ037's probe moved accordingly (`PROBES_V2`). |
| P3-1 | A build not from the pinned v1 went green with the real gates off. | `main` (the build path) returns 1 when the build is not from the pinned inputs. |
| P3-2 | The artifact was kept even for a failing build. | `if: success() && set == coast-v2`. |
| P3-3 | Index `q: 100000` was misleading for the unchanged cells. | Added `q_note`; every file carries its own q. |
| P3-4 | Nothing asserted that v2 land stays off the region box lines. | `--check` counts v2 vertices within 5e-5 deg of a box line in a real build (0 in S1). Test added. |
| P3-5 | `decode_cell` grew `sys.path` on every call. | It now adds the path once. |
| P3-6 | The docstring claimed a box clip that is not done. | Wording fixed. |

The P2-2 test covers these upload cases:
1. not real: refused;
2. tile hash unpinned: refused;
3. no tier-1 pin or a wrong one: refused;
4. a failing check: refused;
5. all proven: exactly one upload, of this directory.

## Verified by the reviewer (no finding)
- **Pixel-class rule:** z15 tile, else upsampled ancestor, else GSHHG by pixel centre. Box masking by pixel centre.
  Apron stripped.
- **`mask_polygons`:** exact. Diagonal pixels stay separate.
- **Specks:** dropped and filled as specified.
- **Orientation:** shells CCW, holes CW.
- **Cell clip:** uses v1's own `clip_half`, so clip points at -155 and 20 N are identical on both sides. Holes travel
  with their shells.
- **Kept v1 pieces:** every v1 piece outside the boxes is present ×10, byte-exact.
- **v2 pieces:** none invalid.
- **base_v1 proof:** sound. A partial or corrupt v1 input fails.
- **Workflow:**
  - pipefail holds;
  - versions are pinned in both jobs;
  - the other sets are unchanged.
- **`decode_cell`:**
  - equals `point_forecast.decode_coast`, and a v1 rebuild is byte-identical to `alert_coast.json`;
  - every non-HI zone in the v2 file is identical to v1.
- **Client:**
  - the page is byte-identical with the flag off;
  - the set name is whitelisted;
  - the script tag comes before `overlay.js`, and there is no CSP to break;
  - forecast points, the exposure tool, the fill mask and the wind stations stay on v1;
  - decode limits and Float32 precision are ample.
