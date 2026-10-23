# G34: location aids (plan section 51), adversarial review

## G34a: the data builders (step 1, commit 5cc1076)

One fresh-context reviewer (Opus 5.5, effort high), 2026-10-23. Scope:
- `tools/places/build_places.py`, which builds `static/places/v1`;
- `tools/coast/build_coast.py --set lakeoutlines`, which builds `static/coast/lakeoutlines-v1`;
- the `coast-build` workflow;
- the tests.

The reviewer worked read-only on the repo, in its own worktrees, from the pinned sources.

**Result: 0 P0, 1 P1, 1 P2, 9 P3.**

### Checked and found correct
- **Rebuilds:** the reviewer's rebuilds equal the author's byte for byte (the index differs only in build time).
- **Borders, tier 1:** an independent decoder found all 440,058 cut edges equal to the Natural Earth source, each in
  its own cell. The crossing points are shared exactly between neighbouring cells.
- **Borders, tier 0:** every vertex is within 0.00425° of the source (tolerance 0.0047°), and every source group is
  present.
- **Classes:** the admin-0 filter drops only the 6 overlay limits, 3 lease limits and 1 unrecognised line. The admin-1
  filter drops only the 6 unrecognised lines. Every US state feature is kept.
- **Places:** all 7,342 Natural Earth places are published once. An independent GeoNames filter, dedupe and cap gives
  the builder's exact counts. Names, ranks, cells and rank order are valid.
- **Lake outlines:** an independent GSHHG reader gives the same selection and water area. Random-point tests at 95,300
  points differ only within 0.0003° of a shoreline.
- **Existing sets:** the land set and the Great Lakes set are byte-identical between 5e4946e and 5cc1076 on the real
  archive, and both indexes equal the published ones apart from build time.
- **Workflow:** the pinned downloads, the heredoc dedent and pipefail all work.
- **Licences:** the GeoNames notice meets CC BY 4.0.

### Findings

| Id | Level | Finding |
|---|---|---|
| F1 | P1 | The GeoNames dedupe (same folded name within 5 km) misses 817 twins of Natural Earth places, 65 of them over 1 million people. Examples: Tokyo twice 6 km apart, New York / New York City, St. Petersburg / Saint Petersburg, Copenhagen / København. From about zoom 10 both labels show, and the files are immutable once published. The author confirmed it on the built cells. |
| F2 | P2 | `--check` passes a self-consistent wrong build. These all pass: tier-0 country borders all deleted; whole cells missing; places cells cut to 100; a tier-0 header with q 10000 over q 1000 data; the largest lake-outline cell removed. The count gates read the builder's own numbers from the index, there is no tier-0 line probe, and the header q is never compared. |
| F3 | P3 | 61 mutants of the reviewer's own, 26 survived. Real gaps: `files_of` without `world-places.bin`; a tier-0 digest over the lines only; two antimeridian crossings in one line; a stream ending inside a varint; islands of river-lakes; ranks for PPLA and for "Admin-0 capital" without adm0cap; region capitals; a missing min_zoom; asciiname as a key; format characters; the rstrip after the cut; a pole place; Douglas–Peucker normalisation; and seven `check()` branches. |
| F4 | P3 | Dropping river-lakes also removes Poyang Lake (inside the Yangtze river-lake), Lac Saint-Pierre and two Volga reservoirs. A taste call for the owner. |
| F5 | P3 | lines-v1 carries no class. Disputed lines, lines of control and indefinite lines (Crimea, Kashmir, Western Sahara, the Korean MDL) draw like settled borders. The 16 US "boundary indicator" lines cross water (Chesapeake Bay, Long Island Sound, the Columbia mouth). Adding a class later means a new prefix. |
| F6 | P3 | A local `--upload` does not require the real-build gates, and `--check` does not say whether they ran. |
| F7 | P3 | A rerun with a newer GeoNames file is refused under v1, but the refusal prints two identical tier-0 hashes. The README promises a new prefix that the workflow does not offer. |
| F8 | P3 | The licence's list of changes omits the per-cell cap and the derived fields. Three README details are wrong: the GeoNames date, the largest cell, and the probe list. |
| F9 | P3 | `read_geonames` splits rows with `str.splitlines()`, which also splits on U+2028/U+2029/U+0085, so a town could be lost silently. No effect on today's file. |
| F10 | P3 | GeoNames twins within GeoNames: 38 same-name pairs within 2 km, e.g. Makakilo / Makakilo City. Merged census-area names with " / " remain, e.g. "Aliamanu / Salt Lakes / Foster Village". |
| F11 | P3 | 484 source fragments shorter than one quantum (about 11 m) cannot be stored. Invisible; the README should say so. |

### Author's verification
- **F1:** confirmed in the built files: Tokyo at tier 0 and in cell 30_130, New York at tier 0 and New York City in
  cell 40_-80, St. Petersburg and Saint Petersburg in cell 50_30.
  - The full file `ne_10m_populated_places.geojson` at the pinned commit (sha256 9b8e3de0…116b) holds GEONAMESID for
    6,797 of the 7,342 places.
  - Its NAME, NAMEASCII, MIN_ZOOM, POP_MAX, FEATURECLA, ADM0CAP and positions equal the simple file's for every place.
  - So the builder can read places from the full file alone, with no join.
- **F2:** confirmed by reading `check()`. The gates use `index["counts"]`, LINE_PROBES scan tier 1 only, and nothing
  compares the header q.
- **F3–F11:** confirmed by reading the code.

### Fix round scope (G34a)
1. **F1:**
   - Read places from the full Natural Earth file, pinned by sha256 in the script and the workflow.
   - Drop a GeoNames row whose id is a Natural Earth GEONAMESID.
   - Otherwise drop it when a Natural Earth place within 5 km has its name, ascii name or one of its alternate names
     (folded).
   - GeoNames-internal twins (same folded name within 2 km): keep the larger.
   - Drop names containing " / " (F10).
   - Add a real-build twin probe: no second "tokyo", "newyorkcity" or "saintpetersburg" within 20 km of the Natural
     Earth place.
2. **F2:**
   - Pin the exact sha256 of the places tier 0 (Natural Earth only) and of the lake-outline tiers 0 and 1. Both are
     deterministic from pinned sources.
   - Recount from the decoded files: Natural Earth places in tier 0 + cells = the source count; GeoNames decoded =
     kept − capped; a band for the cell count and the decoded line count.
   - Always check that the header q equals TIER0_Q / Q.
   - Run the line probes on tier 0 too.
3. **F3:** tests for every real survivor.
4. **F5:** owner's call on the line class and the over-water state lines.
5. **F6:** `--upload` refuses unless the real-build gates ran, and `--check` logs "real-build gates: on/off" (places and
   coast).
6. **F7:** the refusal prints the tier-1 hashes too. The README says v1 is published once; a GeoNames refresh is a new
   prefix plus a client release.
7. **F8 and F11:** licence and README wording.
8. **F9:** split rows on "\n" only.
9. **F4:** owner's call.

### Owner's calls (2026-10-23)
The two owner questions (F4, F5) were asked with a recommended answer each; the owner set the question aside and asked
to continue, so the fix round takes the recommended answers. Both are recorded here as assumptions the owner can still
reverse (each is one constant and a new data prefix):
- **F5:** disputed, indefinite, line-of-control and frontier lines are kept as their own kind (2), so the map can draw
  them apart from settled borders. The 16 US "boundary indicator" lines across water are left out.
- **F4:** river-lakes stay out of the lake-outline set. Poyang, Lac Saint-Pierre and two Volga reservoirs lose their
  outlines with them; the README names them.

### Fix round (G34a)
Every finding is fixed (F4 and F5 as above).

| Id | Outcome |
|---|---|
| F1 | Places read from the full Natural Earth file (pinned by sha256 in the script and the workflow). A GeoNames row is dropped when its id is a Natural Earth GEONAMESID (6,406 rows), when a Natural Earth place within 15 km has its name or ascii name (392), or when one within 5 km has one of its alternate names. Names are folded with a table for ø, æ, œ, ł, đ, ð, þ, ı, ħ, ŧ, ŋ (NFKD does not split them). The 15-km radius for the name: Natural Earth points sit up to about 15 km from GeoNames' (Ganzhou's two points are 7.8 km apart). GeoNames twins within 2 km: the larger kept (39 dropped). Names with " / " dropped (24). Six twin probes in the real gates (Tokyo, New York, St. Petersburg, København, Jakarta, Buenos Aires: no GeoNames twin within 20 km). An independent count on the real build finds 0 same-name Natural Earth / GeoNames pairs within 15 km (the reviewer found 817 such twins before the fix). |
| F2 | The real gates now: the exact sha256 of the places tier 0 and of the tier-1 lines; the Natural Earth places and GeoNames towns counted from the DECODED files (equal to the source count, and to kept − capped); 270–310 cells; the line probes in both tiers, including the Kashmir line of control as kind 2 and no state line across Chesapeake Bay; the header q of every file checked always. The lake-outline set pins the sha256 of both tiers. Each of the reviewer's five passing wrong builds is now refused (tests). |
| F3 | Tests for every real survivor of the reviewer's list. The author's own mutants of the fix-round code (scratch `places/mut2_list.py`, run on a scratch copy after pytest): 89 of 89 killed; 7 survived the first pass (the alternate-name radius, the grid's latitude reach, the two recounts, the lines-hash comparison, the GeoNames ascii-name key, the tier-1 hash in the refusal) and are killed by tests added in this round. |
| F6 | `--upload` refuses a build without the real-build gates (places: built from the pinned Natural Earth files; coast: from the pinned GSHHG archive). `--check` logs "real-build gates on" or "OFF" for every set. |
| F7 | The refusal prints the published and local hashes of tier 0 and tier 1. The README says v1 is published once and a refresh is a new prefix with a client release. |
| F8 | The licence names the full Natural Earth file and the disputed-line note, and lists every GeoNames change (the 8,000 cap, the derived rank and zoom, the dedupe rules, the " / " names). README dates, sizes and probes corrected. |
| F9 | GeoNames rows are split on "\n" only; a test row with a U+2028 in its alternate names stays whole. |
| F10 | Covered by F1 (twins, " / "). |
| F11 | The README says sub-quantum fragments are dropped (about 11 m in tier 1, 110 m in tier 0). |

Real build after the fix round (GeoNames file of 2026-10-23), `--check` 0 problems with the real-build gates on:
- **Tier 0:** 341,711 B of lines (2,721 lines, 144,331 vertices) and 144,182 B of places (6,924).
- **Tier 1:** 285 cells, 4,287,968 B; the largest are 40_0 (45 KB of lines, 185 KB of places) and 40_10 (60 KB, 182 KB).
- **GeoNames:** 138,786 read, 24 " / " names, 6,406 by id, 392 by name, 39 twins, 131,949 kept, 5,120 over the cell cap.
- **Lake outlines:** unchanged from step 1 (tier 0 806fd453…, tier 1 826939b5…), now pinned.

Tests: places 36, coast 33; the land and Great Lakes sets still byte-identical to their goldens.
