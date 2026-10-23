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

## Step 7: test-site verification (2026-10-23, Opus 5.5 MAX)

`test` @ 92300dd (cherry-picks of c3550c5, 32be143, 5ac1902, d26c6ee and the golden c7fcca0; README kept as the test
site's own). Deployed 07:13:38 -> 07:13:44 UTC with 0 failed requests over 19 polls; the served `places.js`,
`forecast.js`, `tools.js` (UI 1.25.0) and `overlay.js` / `overlay.css` (2.22.0) = the commit; the old asset URLs 404.
Site files = `feat/location-aids` except the test site's 3 logo lines. Headless Chrome 155 with the GPU, its own profile
(no account), trusted input. Scripts and outputs: scratch `places/v7` (`s/*.js`, `out/*.txt`, `shots/`). The test branch's
suite: 949 passed, 5 failed, the same failures as before this release: the 4 flag tests that compare the test site's logo
page with the golden (`test_overlay_models_env_and_the_page` fails at test 984979e too) and the numpy-import test.

### What held
- **First visit** (1280x800, empty cache): the forecast at 244-398 ms; the location data only after the load event
  (999 ms) + 1.5 s: index 2,500 ms, then the two world files (3 requests, 491 KB, all from the frame host with CORS for
  the test origin, immutable). Gear rows in the planned order (gridlines, borders, states, names, isobars, lake outlines,
  particles); credits "Borders: Natural Earth" and "Places: GeoNames CC BY 4.0"; no console problems.
- **Border gates** (25 landmark cases chosen from the Python decoder, `s/expect.py`): countries from tile zoom 3,
  disputed lines with them, states from 5, a line from its own zoom (mz10 < z x 10 + 5: Macau's line at 6, Chilean state
  lines at 6, New Zealand's at 7), tier-1 lines from 8 (Ecuador's at 8, none at 7): all as the rules say. States off ->
  only country lines; borders off -> only state lines (country and disputed gone); tiles cleared before every paint.
- **World copies / date line**: the Alaska-Yukon line drawn in the world, one world east and one west; a view on the
  date line has lines and names on both sides (Magadan / Nome). No real border line reaches 180 (the split path exists
  only in the unit tests).
- **Lake outlines** (`s/v4_lakes.js`, `s/v4b_gate.js`): eight views (Great Lakes z6, Superior + Isle Royale z8,
  Okeechobee z8 and z4, Caspian z5, Baikal z6, Titicaca z8, Victoria z6): with the row on vs off, every changed pixel lies
  on an edge of the tile's lake mask (0 elsewhere) and round the named lake, Isle Royale's shore included. Downloads in a
  fresh tab: none under wave height; none under wind at tile zoom 3; the index + overview from 4; cells from 7; nothing new
  under wave height at Okeechobee.
- **Nothing else changed** (`s/v5_identity.js`): the model tiles of the test site (2.22.0) = production (2.21.7) pixel for
  pixel in 12 field x view cells (wave height, period, SST, and wind with the outlines off; Great Lakes z6, Hawaii z7,
  Europe z5; same run, valid time, lake run and SST day): 324 tiles, 0 different. With the lake-outline row ON
  (`s/v5b_identity_on.js`), wave height and SST at Great Lakes z6 and Okeechobee z8 = production too: 96 tiles, 0 different.
- **ECMWF** (`s/v4c_ecmwf.js`): under ECMWF's wind the outlines draw by the same rule (Great Lakes z6: 21 tile masks,
  51,086 changed pixels, all on lake-mask edges).
- **Overlay off** (`s/v12_flagoff.js`, the page served locally with `MODEL_OVERLAYS` unset): without a data address the
  three rows show (ticked), nothing of the location aids is requested, console clean; with `PLACES_BASE` set (a local CORS
  relay of the published data) borders (34,681 line pixels at Europe z5) and 66 names are drawn without the overlay module
  (index + the two world files).
- **Toggles** (`s/v7_toggles.js`, trusted clicks): each row off -> stored "0", drawn nothing of its kind, kept across a
  reload; on again -> back after a reload; the borders credit stays while states are on (one credit for both lines).
- **Phone** 375x812 (touch): at most 60 names (Paris z10.4: 60), no name ever takes a tap (elementFromPoint), no sideways
  scroll, the gear panel 380 px tall with all nine rows visible, City names off/on by touch.
- **Cost of moving the map** (`s/v9_pan.js`, 36 pans / zoom steps at 1920x1080, CPU profile): `places.js` itself 41-59 ms
  in all (about 1.2-1.6 ms a step: update, candidatesFor, tilePaths, drawTile); with its layout and painting the main
  thread is busy 7.7-9.5 ms a step more than production; long tasks: overlay Off 0 vs 0, wave height 1 (58 ms) vs 2
  (126 ms) on production.
- **Playback** (`w54p.js` = the section-42 acceptance script with location-aid cells; production 2.21.7 back to back; a
  collected heap before play; the run's frames warmed at the edge for both Origins; GPU): every cell 7.5-8.0 frames/s with
  0 skipped and 0 unpainted landings on both sites (test / production): Europe z5 wave height shark 1080p 8.0 / 8.0 (the
  first test run 7.5: 234 frames cold at the edge), Europe z5 wind 8.0 / 8.0, Hawaii wave height 8.0 / 7.9 (production:
  one frame took 1,021 ms), the phone (375x812) Great Lakes wind 8.0 / 8.0, Great Lakes z7 wind 7.8 and 7.9 / 8.0 on
  2026-10-22. A network dump on 2026-10-23 (`DUMPNET=1`: every request during playback, any host) settled the Great Lakes
  cell: with the full-resolution wind frames warmed too (the warm-up of the 22nd left them out), it ran 8.0 / 8.0 frames/s
  twice on each site with no landing gap over 300 ms, and on both sites the only requests during playback were the model
  frames, on one connection: the location aids fetch nothing while a loop plays. The two slow test runs of the 22nd came
  from the frame host (193 cold frames; one edge hit of 911 ms). Memory (page heap incl. array buffers, after GC, against
  the overlay Off): live within production + 10 MB in every cell (+0 to +5.6 MB); peaks within production's range + 10 MB
  except one Great Lakes z7 wind run on the 22nd (+77.3 against production's +58.7 / +60.3; that run's live set +3.6:
  garbage-collection timing); the two runs of the 23rd peaked +67.1 / +64.9 against +60.1 / +59.2. With the overlay Off
  the page holds ~2.9 MB more (the decoded border lines and names). Main thread in the Great Lakes z7 wind cell 786-840
  ms/s against production's 749-782 (the lake-outline edges, the borders and names).
- **Names placement** where nothing interfered (`s/v3_labels.js`: an independent re-run of the documented rule from the
  Python-decoded places and the drawn spans' own font): France z6, Paris z8 and z10.4 (the 120 cap reached), New York z9,
  Japan z6, Fiji z7, Great Lakes z6, world z3, Europe z3.7: the page = the re-run exactly (names, places, capitals, widths
  within 1.5 px, no overlaps).

### Findings
- **V7-1 (P1) names laid out against stale station icons.** The page rebuilds its station icons 150 ms after a move
  ends (`refreshWrappedMarkerCopies`, debounced) and when the wind / tide / live feeds land, but the names are laid out in
  the next animation frame after the move and nothing tells them when the icons change. So whether a name gives way to an
  icon depends on timing: in 9 of 20 views the page drew names that sit under an icon (Honolulu under a tide icon at Oahu
  z9 on the phone; London, Boston, Mexico City, Paris under live-buoy dots at zoom 2-5), and in other loads the same names
  were left out (the default view: Honolulu and Hilo have no name, their harbour buoys block them). Fix: the module
  re-lays the names when the marker pane's icons change (a MutationObserver on the pane, one frame later).
- **V7-2 (owner) station icons vs names.** With the timing fixed, the rule as built ("icons win") leaves the main towns
  out wherever a buoy, tide gauge or wind station sits at the harbour: at the default view (Oahu z6) no name at all; Oahu
  z9 60 of 70 names left out behind wind flags; Europe z5 London, Dublin, Amsterdam, Barcelona; US east z6 Boston,
  Providence, Norfolk; world z3 Paris, London, Madrid, Washington. Honolulu's point has four buoy dots round it at zoom 6,
  so no placement on another side saves it while icons hide names. Rules to choose from (preview sheet
  `shots/places_step7_rules.jpg`, `s/v11_preview.js`: the documented placement re-run after the icons had settled, with
  V7-4's fit rule in all four): C as built; B icons never hide names (the icons sit on top of them); D the same placement
  with the names drawn above the icons (names take no taps, so the icons stay clickable); A big places stay (ranks 0-2:
  national and state capitals, cities of 1 M+, ignore icons), every other name tries the left of its dot when the right
  meets an icon. Names in the whole view (C / B / D / A): Hawaii z6 0 / 2 / 2 / 1, Oahu z9 0 / 16 / 16 / 1, Honolulu
  z11 15 / 36 / 36 / 19, Europe z5 45 / 55 / 55 / 56, US east z6 48 / 54 / 54 / 56, world z3 27 / 38 / 38 / 38. Under B
  a name whose dot sits at a harbour loses letters under the icons (Honolulu, London, Boston); under D they read whole;
  under A Oahu at zoom 9 keeps only Honolulu (the 40-px wind flags cover the rest of the island).
- **V7-3 (owner) Natural Earth names.** The builder reads Natural Earth's NAME (the plan said NAME_EN first): local
  spellings (København, Göteborg, Montréal, Ōsaka, Zürich) and a few Natural Earth typos (Shenyeng, Starsy Oskol,
  Nizhenvartovsk). NAME_EN differs for 950 of 7,342 places and has its own errors (Mazatlán -> "Juarez", Dongola ->
  "Karma", Hami -> "Yizhou"). Any change of names is a rebuild under a new prefix (static/places/v2: v1 is immutable) plus
  a one-line client change; GeoNames towns keep their own spelling (Kīhei, Lā'ie).
  Measured against a third source (scratch `places/names3.py`..`names5.py`: GeoNames' own name for the Natural Earth place,
  matched by its `GEONAMESID`, kept only when GeoNames' position is within 25 km: 28 of 6,480 matched ids point at another
  place): of the 950 places where NAME and NAME_EN differ, GeoNames gives NAME_EN's spelling for 378, and for 236 of
  those that spelling is within 2 letters of NAME: Natural Earth typos
  and transliteration variants (Shenyeng -> Shenyang, Nizhenvartovsk -> Nizhnevartovsk, Banghazi -> Benghazi, Poitier ->
  Poitiers, Ormac -> Ormoc, Bogota -> Bogotá, Ōsaka -> Osaka) and one wrong town (Mazatán, Sonora, named "Mazatlán").
  The other 142 are exonyms or long official forms (Copenhagen, Gothenburg, Saint Petersburg, Fort Worth, but also New
  York City, Las Palmas de Gran Canaria, Acapulco de Juárez, "Washington" for "Washington, D.C."). Starsy Oskol is in
  neither (GeoNames "Staryy Oskol"): a hand fix. GeoNames' own names alone are no cleaner (610 differ from both: Sitten for
  Sion, "Ti Port-de-Paix", Nepali names with combining marks).
- **V7-4 (P2) names cut at the map's edges.** Candidates include places up to 40 px outside the view (meant for
  stability while panning), and a name may run off the right edge: a place just left of the view shows only the end of
  its name ("…nsport", "…isburg", "…ork" down the left edge of the phone at US east z7; "…gham" at Europe z5; world z2.4
  draws "Cairo" in both edge copies), and "Har…" (Hartford) runs off the right. Frequent on phones. Fix: a name is drawn
  only when its dot and its whole text fit on the map, trying the left side of the dot when the right runs off.
- **V7-5 (P3) lake masks kept when the outlines are not wanted.** `_syncLakeOutlines` only attaches the lake-outline set:
  when outlines stop being wanted (wave height, period, SST, or tile zoom under 4) the set stays on the layer, so tiles keep
  their 64-KB masks and cells already queued still download. Nothing is drawn from them (the draw is gated on the field:
  the identity checks above). Under ECMWF wave height at Great Lakes z6, 21 tiles still held a mask. Fix: detach the set
  (`setLakeOutlineCoast(null)`) when not wanted, which clears the masks.
- **V7-6 (P3) credits without data.** With no data address (overlay off and `PLACES_BASE` unset) the rows still show and
  the map's credits name Natural Earth and GeoNames although nothing can load (`sync()` credits a kind whenever its row is
  on, also without a store). Fix: credit a kind only when the store exists (and once its data has loaded).

### Owner's calls (2026-10-23)
- **V7-2: D**, the names drawn above the station icons, placed as in B: the icons are no obstacle, a name takes no tap.
- **V7-3: fix spelling errors only**: local spellings stay; a Natural Earth NAME is replaced by NAME_EN where GeoNames
  (by `GEONAMESID`, within 25 km) gives the same spelling and it is within 2 letters of NAME (236 places), plus Starsy
  Oskol -> Stary Oskol by hand. A new data prefix `static/places/v2` (v1 is immutable), published by the owner.
- **Order: fix first, then G34** (the reviewers see the final code).

### Fix round scope (step 7b)
1. Names above the icons (V7-2 D): the label pane above the marker pane (z 620: under tooltips 650 and popups 700, under
   the map's controls), names keep `pointer-events: none` (an icon under a name still takes the click or tap); the
   station icons leave the blocked rects (controls, the overlay's sheet and the gridline labels stay).
2. V7-1 as left by D: the names are re-laid when a panel changes size (a ResizeObserver on the map's control corners
   and the overlay's sheet), not only on map moves.
3. V7-4: a name is drawn only when its dot and its whole text lie on the map; when the right side runs off, its left
   (a `place-left` class: text before the dot); the rest of the placement as built.
4. V7-5: the lake-outline set detached when outlines are not wanted (`setLakeOutlineCoast(null)`: masks cleared, queued
   cells dropped); overlay asset 2.22.1.
5. V7-6: a kind credited only while its data has loaded (store `ok`).
6. V7-3 data v2: the builder's name pass (the agreement rule + the hand list, counts reported by `--check`), the real-
   build pins updated, coast-build `set: places` uploading to `static/places/v2`; a job-only push (owner's go-ahead), the
   owner's dispatches (check, then publish), the bucket verified; then the page's base `static/places/v2`.
7. Tests for each item (unit + the page harness), mutants, UI 1.25.1, the golden re-captured in its own commit, README.
   Then the test site (MAX), then G34.
