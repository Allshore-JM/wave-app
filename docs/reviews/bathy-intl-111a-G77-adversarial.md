# G77: adversarial review of plan section 111a (bathymetry, Australia and New Zealand)

Reviewed: `git diff 6cc03ff..6014a9b` on feat/bathy-intl (2026-11-27), one fresh reviewer, read-only, briefed with
the change's intent and the code only. Result: **no P0, no P1**; 5 P2 and 4 P3, all fixed below.

The reviewer confirmed by running: `tests/bathy` 362 pass / 1 skip; the LINZ pin (65 items, digest 03e9b444...)
against the cached collection; every one of the 65 sheets wholly inside exactly one NZ box; the NSW strips' CRS,
nodata and footprints inside the NSW boxes; the pre-pass on DATA_32028 (53 blocks found against 19 holding low
ground at near-full resolution: none missed); the datum sign (-25 m NZVD2016 with MSL -0.15 m -> 24.85 m); the NZ grids
finite over their whole box and covering every corner block of the regions with a cell to spare.

| # | Sev | Finding | Fix |
|---|---|---|---|
| 1 | P2 | With `clip` and the `_hits(whole)` filter, `outside_box` could never fire for the new kinds: a sheet a renewed LINZ pin brings in outside every NZ box would be dropped without a word. | `build_bathy.sheets_in_regions`: every LINZ item and every NSW archive (manifest `bbox`) must lie inside the box (+ reach) of a region reading the source, else the build stops. Test `test_a_sheet_outside_every_region_stops_the_build`. |
| 2 | P2 | The stated MHHW range (0.56-1.49 m) was the gauges', not the grid's: the grid falls to 0.277 m off Makara (Cook Strait), 0.34-0.47 m under sheets BP31 / BQ31 / BP32. | Text corrected to 0.28-1.49 m (licence text, source comment). The grid test now holds the minimum over each NZ box at >= 0.25 m. EOT20's 1/8-degree cells in the strait are not checked against a gauge there (no UHSLC gauge; LINZ secondary ports Mana / Paraparaumu would be the check): recorded as an open item for the owner. |
| 3 | P2 | The read-back samples only pixels already written, so a sheet whose water was dropped (judged land, not joined to the sea) still passed; several sheets stop 1-3 m deep and rely on the GSHHG seed. | `bathy_readback.sheet_cover`: per sheet, random source pixels at or below the high-tide line looked up in the built tiles; the share drawn as water printed and sheets under 0.5 named. The seed comment corrected. |
| 4 | P2 | The STAC pin covers the item documents, not the COG bytes. | Stated in `bathy_stac.py`'s docstring (LINZ updates the item with its file; residual risk). |
| 5 | P2 | Each NSW half downloaded and hashed all 10 archives (1.04 GB). | Each manifest archive carries its lon/lat `bbox`; `fetch_mirror(box=...)` reads only those meeting the region's read box (au-nsw-n 5, au-nsw-s 7 on the runner). |
| 6 | P3 | A GeoTIFF failing the size check after the rename kept its final name and would be reused. | `extract_member` checks the size of the `.part` file, deletes it on a mismatch, renames only a whole member. |
| 7 | P3 | One damaged download of an archive stopped the build. | `fetch_mirror(tries=2)`: a copy whose size or hash differs is fetched again once. Test with a damaged first copy. |
| 8 | P3 | `build_tide_grids.py` said the model planes were taken "exactly as" the gauges', which used a parabola peak fit. | Docstring corrected (the gauge correction absorbs the few millimetres). |
| 9 | P3 | The notice's NZ coverage list missed Whanganui (BL32) and the Waikato coast (BC31, BG30/31). | List corrected ("parts of ... the Waikato coast ... Whanganui ..."). |

Checked and correct (reviewer): `check_datum_grid` on the NZ grids (local overrides only in tests); the STAC id /
count / digest / item checksum / href forms; zip extraction by exact member name to a hash-named path, size-capped,
CRC enforced; the mirror never overwrites (paginated key list; `static/bathy-src/` outside `upload()`'s prefix); the
pre-pass tile formula, overview scaling and float32 nodata comparison; the five boxes overlap no other region
(pacific-sdb's box meets au-nsw-n at 28-28.5 S with no DEP tile within 0.5 degrees, prio 5 under 20); the client's cap
of 256 and its `short` check; the workflow region lists; the temporary dry run (no secrets, read-only, no upload).
