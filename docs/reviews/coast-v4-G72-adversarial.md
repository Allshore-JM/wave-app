# G72: adversarial review of coast set v4 (plan section 104, release 2)

Reviewed: feat/coast-v3 at 3f845da (55f28c9 + 3f845da), against production fe8d59b (coast v3 live). One fresh reviewer,
read-only, 2026-11-20.

Scope: the set v4 (v3 + the US West Coast: us-ca-south, us-ca-north, us-or-wa as seam regions, Puget Sound excluded) in
build_coast_v2.py (seam margin and cell-stage cut, exclusion boxes, the 0.25-degree split of big pieces, the quadtree
GSHHG fill, the seam-line step gate, the size gates, SETS["v4"]), build_alert_coast.py (--coast v4, dissolve_cuts),
coast-build.yml set coast-v4, alert_coast_v4.json.

Checked sound by the reviewer:
- v2 and v3 stay byte-identical: margin, exclusions and the split term enter the format key and index.json only with
  a seam region; v3 cells take the exact path; v4 pins v3's twelve cells byte for byte (SAME_CELLS).
- The upload path: prefix tied to the set, check + is_real (all tile hashes pinned) + the pin before any upload; a
  runner build that differs from the pin is refused, never published.
- The workflow's coast-v4 cases and artifact paths.
- gshhg_fill: exact predicates, boundary centres go to the per-pixel leaf, degenerate rectangles subdivide.
- split_big: valid pieces, cut edges on the piece's own bbox edge (the client's same-pixel exemption keeps them exact),
  even-odd parity kept; dissolve_cuts closes every chain; a step can never be cancelled by a cut twin.
- Client limits (8 MB, 4M vertices) well above the 2 MB / 600k gates.
- No P1.

| # | Sev | Finding | Fix |
|---|-----|---------|-----|
| 1 | P2 | A zoom-15 pixel edge can round onto a 0.25-degree split line at q 1e5. A real shore edge there was skipped by densify(split=0.25) and had no twin, so four hazard lines had 16-27 m gaps (CAZ043, CAZ505, CAZ509 x2). | The alert builder no longer skips edges on split lines: dissolve_cuts removes the exact twin pairs (every cut edge has one: 4,842 twins in the four split cells) and the 38 lone edges left on those lines are real shore, kept as coast. Test: a real shore edge on a split line stays coast. |
| 2 | P3 | Exclusion-box steps were measured only because -123.2 and 46.9 happen to round onto a pixel edge. | With #3 the exclusion is cut exactly on its box lines at the cell stage, so the gate measures them exactly (v4: 38 edges along seam and exclusion lines, longest 521 m, gate 600 m). |
| 3 | P3 | "Puget Sound left to GSHHG" was GSHHG rasterised at zoom 15, speck-filtered and simplified, not v1's rings; Puget zones' hazard lines moved against v3. | The cell stage uses the traced land only in the boxes less their exclusion boxes, and v1's land elsewhere, so Puget Sound is v1's own rings, cut exactly on the exclusion lines (45_-125: 535k -> 440k vertices). Test: an islet inside an exclusion box comes out as v1's ring, vertex for vertex. |
| 4 | P3 | The 3 % area gate is mostly inland GSHHG fill (~40,000 km2): a bay flipped to land would pass; no probes in the bays. | Eleven bay and estuary probes: SF Bay (2), Angel Island, San Pablo Bay, Suisun Bay, Franks Tract, Sherman Island, Humboldt Bay, the Columbia mouth, Willapa Bay, Grays Harbor (25 West Coast probes in all). |

After the fixes:
- The laptop v4 build checks clean at 6e5740bf (25 West Coast probes, seam and exclusion lines 38 edges, longest
  521 m, size gates met: biggest cell 45_-125 1.0 MB / 440k vertices, biggest piece < 15k). The runner's dry run of
  the code before these fixes (#742, 55f28c9) differed from the laptop pin by a few bytes in the GEOS-joined West
  Coast cells, as expected; the runner's build is the reference, pinned from the next dry run.
- alert_coast_v4.json: every zone outside CA/OR/WA identical to v3; the Puget Sound zones' lines on v3's (median 0 m;
  run ends may move a few hundred metres); West Coast lines 2,847 -> 3,191 km, 126 -> 259 lines (creek mouths and
  inlets that GSHHG closes off now break the open-coast line, as in Hawaii; 25 small closed loops are sea stacks).
- Suites: coast 70, alerts 44.
