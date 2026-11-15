# G64a: the Pacific satellite depths builder (plan section 97)

One fresh reviewer, read-only, on `feat/sdb` @ 946c6a6 (`git diff fd6663d..946c6a6 -- tools/bathy tests/bathy
.github/workflows app.py README.md`). The reviewer built real and synthetic boxes, read the tiles back with its own
reader, served DEP's layout from a local copy to make tiles fail, and ran 26 mutants.

**Result: 0 P0, 1 P1, 0 P2, 7 P3.** Six are fixed, one is documented, one is for the owner.

## Findings and outcomes

| # | Level | Finding | Outcome |
|---|---|---|---|
| F1 | P1 | A listed DEP tile that failed to open (403, 404, a 5xx after GDAL's retries, a timeout, a broken header) gave an empty block list. The build went on, `check` passed, and that tile's reefs were missing with no message. A test pinned the silent behaviour. | Fixed. `open_tile` asks 3 times, then the build stops ("the tile list may be out of date"). The old test is reversed. |
| F2 | P3 | The 12-m cap is applied after the warp averages DEP's 10-m pixels, not before. A few z14 pixels blend a deeper DEP pixel (0.24-1.47 % of pixels, at most ~5 m), and a shallow pixel next to deep water can drop out. | Documented (README, `bathy_sources.py`). The effect stays inside DEP's own ~2 m error and touches only pixels near the cap. |
| F3 | P3 | The exclusion of the US boxes was applied at zoom 14 only. A coarser pixel whose centre lies inside a US box could take DEP water from its children outside it (up to ~300 m at z8). No real data was affected. | Fixed. The cut is repeated after every pooling step and in the coarse levels; a test checks every zoom and every apron. |
| F4 | P3 | Values at or above 0 m would have been published as water. None are in DEP's files today. | Fixed. `usable()` and `read_field` drop them for a `water_all` source. |
| F5 | P3 | The pre-pass judged pixels without the source's offset and used its own fill limit. They agree today (offset 0); a bias correction would have lost blocks. | Fixed. The pre-pass gets the offset and `S.SANE_M` (tests on `usable`, `presence` and the `dep_blocks` call). |
| F6 | P3 (data) | DEP has no data on a ~0.2-degree strip at 180 degrees: Taveuni, Rabi, Qamea and Laucala show GEBCO only. Its own mean / count / stdev layers are empty there too. | For the owner. Not a builder defect. |
| F7 | P3 | Test gaps: no test held water shallower than 5 m (reef flats), and none checked that the build's VRT leaves out the four turbid tiles. | Fixed. A 0.3-m strip, a 0 / +0.5 m strip and a VRT test. |
| F8 | P3 | DEP's STAC dates say all of 2024; the key says `2024-07/2024-12`. | Fixed wording: "a 2024 Sentinel-2 composite" in the notice and README. |

## Checks after the fixes

- Tests: `tests/bathy` 188 passed (venv).
- Mutants (scratch copy): 11 of 11 killed: `usable` keeps values >= 0, ignores the offset, ignores the fill limit;
  `open_tile` never retries; a missing tile read as empty; `read_field` keeps values >= 0; `pass2` and the coarse
  levels never cut; `build` passes no exclusion to the coarse levels; the VRT lists every tile; `dep_blocks` passes no
  offset.

## Runner dry run (temporary workflow `sdb-dryrun.yml`, removed after; job 132687734850, 4 cores)

| Item | Value |
|---|---|
| Pre-pass | 5,297 blocks with a usable pixel, from 495 tiles, 197 s |
| Pass 1 | 5,059 blocks hold low ground, 1,019 s |
| Pass 2 | 134,121 tiles at zooms 11-14, 1,917 s |
| Region | 138,910 tiles (z8 345, z9 716, z10 1,633, z11 3,990, z12 10,945, z13 31,443, z14 87,743), 1,195.2 MB |
| Check | 0 problems |
| Build wall time / max RSS | 36 min 11 s / 829 MB |

## Held up (from the reviewer)

All 499 headers equal the grid the VRT assumes; the pre-pass found every block in a synthetic set of 9,057 pixels placed
on block, tile and 180-degree edges; every water pixel carries the DEP source id; aprons match across the x = 0 / 2^z - 1
seam at every zoom; stage-1 regions build byte-identical under the new code; the notice carries the citation, CC BY 4.0,
"Contains modified Copernicus Sentinel data" and the list of changes.
