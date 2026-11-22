# G73: adversarial review of coast set v5 (plan section 105)

Reviewed: feat/coast-v5 against production 462cb4c (coast v4 live), `git diff 462cb4c..HEAD` up to the runner pin
(eeb8731) and the hazard-line file. One fresh reviewer, read-only, 2026-11-22.

Scope: the set v5 (v4 + the US East and Gulf coasts: us-me-nh a full trace; us-sne-ny, us-nj-ches, us-va-nc, us-sc-ga,
us-fl-east, us-fl-south, us-gulf-east, us-gulf-west two-tier: a fine z15 trace inside the open-coast band
tools/coast/band_v5.json, a coarse z12 bathymetry class elsewhere in the box, GSHHG only outside the boxes) in
build_coast_v2.py (band, owned boxes, two-tier fetch, Tiles(expect), edge_fill, trace_fine / trace_coarse, the East
split 0.125 deg / 5k, the band-line seam gate, seam_allow, SETS["v5"]), make_coast_band.py, coast-build.yml set
coast-v5, the client (COAST_SETS / COAST_SETS_OK v5, asset 2.31.3), build_alert_coast.py (--coast v5, set-only
SHELTERED shores) and alert_coast_v5.json.

Checked sound by the reviewer (each measured or traced):
- The runner's build (f356dd68, dry run #761) checks clean on the laptop; the only allowed step is the 854 m
  Mississippi crossing; longest East step 460 m.
- v4's 20 cells byte-identical: the v5 digests with the 16 East cells swapped back to their v1 hashes rebuild v4's
  pin 58d55125 exactly. v2-v4 format keys match their published index.json; a fresh v3 rebuild gives its pin.
- band_v5.json rebuilds byte-identical (make_coast_band.py --check).
- The band boundary sampled along its whole path (~8,500 km): longest continuous fine/coarse disagreement 479 m; East
  box lines: only the allowed Mississippi stretch over 600 m.
- Split cuts on 0.125 deg lines all have exact reverse twins (dissolve_cuts leaves no false hazard coast).
- Owned boxes, the two-tier tile digest and pins, the upload refusals (prefix, check, pins, band sha, tier-1 pin),
  determinism (sorted iteration, pinned wheels).
- Client unchanged for COAST_SET unset / v1-v4; East per-z12-tile walk peaks at 29k, under live v4's 42-59k.
- The East land/water changes against v1 are the data at MHHW (Laguna Madre flats, dredge islands added; marsh and
  tidal rivers removed), not a code fault.
- Hazard lines: v1-v4 files unchanged by the set-only shores; v5 drops 3 one-line zones.
- No P1, no P2.

| # | Sev | Finding | Fix |
|---|-----|---------|-----|
| 1 | P3 | alert_coast_v5.json was untracked, so a merge would leave COAST_SET=v5 on v1 hazard lines. | Committed at bcb762c (built from the runner's bytes, coast id f356dd68) before the review ended; test_committed_v5_file_* runs in CI. |
| 2 | P3 | The seam gate measures each band-line stretch on its own; band lines are z10 pixel edges, so a disagreement along a diagonal band boundary would be many <= 125 m steps and never trip 600 m. | check() also joins the one-way stretches across staircase corners and cell lines (seam_chains) and gates each chain at 600 m (v5: 9,660 chains, longest 521 m, the West Coast's Puget line; East shorter). Test: a 20-leg staircase of 111 m steps is one ~2.2 km chain; two-way cuts cancel. |
| 3 | P3 | edge_fill had no direct test. | Test: an 'outside' strip on a box line takes the river's water and the beach's land 2 px across the line; pixels farther out stay 'outside'; data far from the line fills nothing. |
| 4 | P3 | The 3 % area gate is set-wide (~10 sq deg); a whole bay flipped to land would pass, and several large bays had no probe. | 16 water probes: Delaware Bay, Long Island Sound, Albemarle Sound, Lake Pontchartrain, Lake Borgne, Barnegat Bay, the Indian River Lagoon, Mississippi Sound, the Laguna Madre, Biscayne Bay, Charlotte Harbor, Apalachicola Bay, Sabine Lake, Matagorda Bay, Narragansett Bay, Penobscot Bay (each water in the runner build). |
| n | note | A seam_allow entry that no longer matches any step would quietly allow a future one there. | check() reports a stale entry as a problem (stale_allows; test). |

Notes, not defects: 45_-75 and 40_-80 count as changed with ~0 km2 of visible change (no bathymetry over Quebec /
the St Lawrence inside us-me-nh's box: GSHHG filled and retraced; bytes only). index.json split.min_vertices reads
20000 although the East cells were cut at 5000 (the East value is under split.east; nothing reads it; changing it
would change the pinned build).

After the fixes:
- The runner's build f356dd68 checks clean with the new gates (chains longest 521 m, all 16 bay probes water,
  seam_allow entry matched): the pin is unchanged (check-only changes).
- Suites: tests/coast 81, tests/test_alert_coast.py 46; full pytest 3,033 + 394 pass (failures only environmental:
  pyproj absent in the build venv for tests/bathy/test_bathy_sdb.py, a buoy timing test that passes alone); node
  1,412 / 1,413 (a radar timing test that passes 3/3 alone).
- alert_coast_v5.json from the runner's bytes: check clean, 362 zones, every zone outside the East and Gulf states
  identical to v4; East/Gulf lines 7,391 -> 8,952 km, 1,006 -> 1,961 lines; gained TXZ336 Coastal Matagorda (Gulf
  beach), MSZ088, NYZ078, MDZ023 (sounds where v4 already draws neighbouring lines); lost GAZ217, MEZ028, VAZ523
  (1-2 km bay pieces).
