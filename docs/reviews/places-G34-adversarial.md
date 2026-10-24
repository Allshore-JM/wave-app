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

### Fix round 7b (2026-10-23, Opus 5.5 HIGH)
The scope above, with two owner comments given with the go: **one "Borders" row** for country and state lines (the
states row and its key `allshore.states.v1` go; the key is removed on load), and **lake outlines always on** under wind
(the gear row "Lake outlines (wind)", its setter and its key `allshore.lakeOutlines.v1` go; the key is removed on load).
- 2cd46e8 (UI 1.25.1, overlay 2.22.1): names in `placesLabelPane` z 620 (above the marker pane, under the tooltips),
  `BLOCKED_SELECTOR` = controls, the overlay's sheet, the gridline labels; `thin(items, blocked, max, size)` keeps a name
  only when its dot and whole text lie on the map, else tries the text left of the dot (`place-left`), else leaves it
  out; a ResizeObserver on the control corners and every `.ov-sheet` (found by a MutationObserver on the map container)
  lays the names out again; credits only while the store is `ok`; `KEYS` = borders and places. Overlay:
  `_syncLakeOutlines` detaches the set when not wanted (another field, SST, tile zoom under 4) and drops its queue
  (`CoastStore.dropQueue`); `setLakeOutlineCoast(null)` clears every tile's mask under any field; `_mountObs` syncs.
- b4286eb the golden (own commit); bebcb0b a playback pin (wind -> SST lets the set go).
- 0578b8d data v2: the builder's name pass (`NAME_FIX_KM` 25, `NAME_FIX_EDITS` 2, the hand list Starsy Oskol ->
  Stary Oskol; `read_geonames_names`, `edits`, `fix_names`; counts `name_fixes_agreed` / `name_fixes_hand`, real-build
  gate 220-250, `NAME_PROBES`, `FORMAT_KEY` `namefix=`), coast-build `set: places` to `static/places/v2`, the page's
  base `static/places/v2`; bf25e67 pins (GeoNames agreement, the no-op case).
- Suites: Node 951, pytest 1,295; mutants: client 27 of 27, name pass 12 of 12. A local real build v2 against v1:
  134,171 places, exactly 235 names changed (234 by agreement + 1 by hand), no other field changed, border lines
  byte-identical, `--check` 0 problems.
- Data v2 published (owner: "go ahead. use my browser to run any required tasks"): rollback tag `prod-pre-places-v2`
  @ 51590bf; `Live-Buoy-Update` fast-forwarded to 11369c8 (job only: the builder, its tests, its README, the workflow's
  places lines; the page unchanged, CI green). coast-build #460 (`set: places`, upload false, 18:21 UTC) = the local v2
  build in every size and count (tier 0 341,711 + 144,202 B, 285 cells 4,287,982 B, name fixes 234 + 1, real-build gates
  on, 0 problems); #461 (upload true) green at 18:27:54 UTC. The bucket: `index.json` = the local build but `built_utc`
  (tier-0 sha 1c226275..., 285 cells), the two world files and three cells byte-identical, a cell without lines 404,
  immutable + nosniff + CORS; decoded: Shenyang, Stary Oskol, København in; Shenyeng, Starsy Oskol out. (P3 nit: the
  published `LICENSE.txt` heading still says "static/places/v1"; fix with the next data version.)

## Test-site verification of fix round 7b (2026-10-23, Opus 5.5 MAX)
`test` @ 7651473 (cherry-picks of 2cd46e8, b4286eb, bebcb0b, 0578b8d; the builder files and the workflow left out of
`test` (they never run there), README kept as the test site's own). Pushed 18:43:43 UTC, live 18:44:43 (one failed
request at 18:44:13: the idle free instance waking, before the switch); the served `places.js`, `forecast.js`,
`tools.js`, `graticule.js`, `winds.js`, `tides.js`, `livelist.js`, `basemap.js` (UI 1.25.1) and `overlay.js` /
`overlay.css` (2.22.1) = the commit; the 1.25.0 / 2.22.0 URLs 404. Site files = `feat/location-aids` except the test
site's 3 logo lines. The test branch's suites for the touched files: Node 255 of 255; the page and flag tests 47 passed
and the 4 known failures (the logo goldens and `test_overlay_models_env_and_the_page`). Headless Chrome 155 with the GPU, its own profile (sign-in check 0),
trusted mouse / touch / keys. Scripts and outputs: scratch `places/v7b` (`s/b*.js`, `out/b*.txt`, `shots/`).

### What held
- **First visit with the two retired keys stored** (`b1`): both removed on load; the data only from `static/places/v2`
  (index at 2,391 ms = the load event at 882 ms + 1.5 s, then the two world files: 3 requests, 491 KB, none from v1);
  gear rows: Lat/long gridlines, Borders, City names, Pressure isobars (wind), Particles (no states row, no lake-outline
  row); credits "Borders: Natural Earth" and "Places: GeoNames CC BY 4.0" once the data is in; by trusted clicks Borders
  off -> every line gone (19 inked tiles -> 0 at US east z6, state lines included), its credit gone, stored "0"; City
  names off -> names and their credit gone, the line layer off with both; both back on -> all back.
- **Names** (`b2`, the independent re-run of the documented rule from the Python decoder's v2 places): 23 desktop views
  and the same 23 on the phone (375x812, cap 60) = the page exactly (names, places within 1 px, capitals, side of the
  dot, widths), no overlaps, none under a control, none off the map's edge. Names now drawn over station icons: the
  default view (Hawaii z6) shows Honolulu and Hilo (step 7: none); Oahu z9 14, Honolulu z11 21, New York z9 61 names over
  icons. Text before the dot where the right runs off (Sevastopol, Vienna, Zagreb, Winnipeg on desktop; Kailua-Kona,
  Boston, Providence on the phone); "off the map" leaves out at most a few edge places per view.
- **Laid out again on a panel change, the map unmoved** (`b3`, trusted clicks): folding the overlay panel frees names
  (Japan z6: Seoul, Pyongyang, Hamhung, Wonsan, Sinuiju), opening it takes them back, the overlay Off frees six, wave
  height again takes them back, the map tools' bar opens and closes; on the phone the sheet folded (four names freed),
  gone (Off: six) and back; after each change the page = the re-run with the new controls; 0 layouts in 8 s at rest.
- **Playback** (`b4`): 15 s of shark (wave height, wind; desktop 1920x1080 and the phone): 0 label layouts, 0 writes to
  the names' pane (the observers watch sizes, not the panel's text).
- **The icons keep the pointer** (`b7`, `b7f`): at 2,280 desktop points and 1,125 phone points inside both a name and an
  icon, the element hit is the same with the names shown and hidden (0 names hit); 38 trusted clicks and 36 trusted taps
  as A/B pairs (names shown / hidden): the same target and window every time; hovers show the icon's tooltip over the
  name; clicks and taps on tide icons, wind flags and live-buoy dots under names open their windows.
- **Lake outlines always on under wind, let go when not wanted** (`b5`): eight lake views under GFS wind and two under
  ECMWF: every changed pixel on an edge of the tile's lake mask, round the named lake; let go through the layer's own
  path (`setLakeOutlineCoast(null)`) every mask is cleared, and given back the tiles are pixel-identical to before;
  wind -> wave height / period / SST: the set off the layer, 0 masks, an empty queue, 0 lake-outline downloads while
  browsing three lake views; wind at zoom 3.4: off (masks 0), back at 6: on (21 masks); Off: chunks dropped. A fresh
  visit under wave height asks nothing of the set (`b5a`).
- **Nothing else changed** (`b6`): the test site's model tiles = production (2.21.7) pixel for pixel for wave height,
  period and SST at Great Lakes z6 and Okeechobee z8 (same run, valid time, lake run, SST day: 144 tiles, 0 different);
  under wind every one of the 51,086 / 2,630 differing pixels lies on a lake-mask edge.
- **Credits only with data** (`b8`, DevTools Fetch refusing `static/places/v2`): no credit, no line, no name, the rows
  stay; the data back: after the 60-s cooldown the next zoom loads it (credits, 19 inked tiles, 58 names); the index and
  lines answering but the places file refused: still no credit. A trace of the store (`b8d`): one failure, the next try
  only after 60 s, a zoom inside the new cooldown asks nothing.
- **Border gates on the v2 data** (`b9`): the 25 landmark cases as the rules say; the one Borders row takes the state,
  country and disputed lines away together (alpha 155 / 192 / 163 -> 0) and back.
- **The corrected names on the page** (`b10`): Shenyang (z6, z9), Stary Oskol (z8, z10), Poitiers, Nizhnevartovsk;
  local spellings kept: København, Montréal.
- **Playback** (`w54p.js`: a collected heap before play, GPU, run 2026102312 warmed at the edge for both Origins;
  production 2.21.7): today's machine state is slow (production itself 7.1-7.8 frames/s at Europe 1080p shark; step 7:
  8.0). Europe z5 wave height shark 1080p, test / production ABBA: 6.2, 6.5 / 7.1, 7.1; then three interleaved rounds:
  production 7.1 / 7.4 / 7.8, test 6.6 / 7.7 / 7.6, the test site with borders and names off 7.5 / 7.6 / 6.5: run-to-run
  noise (the browser's render time per frame 88-118 ms for the same page). The names' new stacking costs nothing
  measurable: names above the icons 7.2 / 7.4, at step 7's level (pane z 360) 7.5 / 7.0, names off 7.1 / 7.5, borders and
  names off 7.7 / 7.6. Great Lakes z7 wind shark 1080p (lake outlines, isobars): test 7.7 / 8.0, production 8.0 / 8.0.
  Every run: 0 skipped, 0 unpainted landings, in flight within the cap, every lake step with its own lake frame. Memory
  (page heap incl. array buffers, after GC, against the overlay Off): live test +39-48 MB, production +36-44 MB (within
  production + 10 in every cell); peaks within production's range + 10 MB (test as served +67-81, production +65-76).
- **Overlay off** (`b12`, the feature branch's page served locally with `MODEL_OVERLAYS` unset): without a data address
  the Borders and City names rows show, nothing is requested or drawn, and no credit (step 7: both credits shown); with
  `PLACES_BASE` = a local CORS relay of the published `static/places/v2`: borders (34,681 line pixels at Europe z5) and
  56 names (step 7: 66; the names that ran off the map's edges are left out now), credited; console clean.
- **Owner's sheet** (`shots/places_fix7b_test_site.jpg`): the default Hawaii view, Oahu z9 and Honolulu z11 (names over
  the station icons), US east z6, the Great Lakes under wind (lake outlines, borders, names, isobars), the gear, three
  phone views (names left of their dots at the right edge).
- Console: clean in every check (the only lines: the refusals staged in `b8`, my own canvas readbacks, and two 503s of a
  METAR history when my A/B taps asked the same airport's 24 hours many times: the route's designed busy answer, retried
  by the window).

### Findings
- **V7b-1 (owner awareness)**: with the names above the icons (decision D) a name can cover part of a wind flag's speed
  number or a buoy dot (Oahu z9: "Pearl City" over a "13"); the icons stay clickable and their tooltips show under any
  name (the A/B checks above).
- **V7b-2 (P3 nit)**: the published `static/places/v2/LICENSE.txt` heading still names `static/places/v1`
  (`tools/places/LICENSE-PLACES.txt`); the objects are immutable, so the fix goes with the next data version.
- Nothing else: every check above as designed.

## G34: two fresh reviewers (2026-10-23, Opus 5.5 MAX; reviewer A on Fable 5.1)
Target: `feat/location-aids` @ da7fed5 (`git diff 5e4946e..da7fed5`: the builders (G34a-reviewed), the data v2 name
pass (never reviewed before), `static_ui/places.js`, the lake outlines in `static_overlay/overlay.js`, the page) and the
test site @ 7651473 (UI 1.25.1, overlay 2.22.1), production (11369c8, UI 1.24.0, overlay 2.21.7) for comparison.
Brief: scratch `g34/brief.md` (owner decisions 1-11, claims C1-C10, known items K-1..K-12). Reviewer A (code and data,
own detached worktree, no browser): `g34/a/g34-a-report.md`. Reviewer B (the test site with trusted mouse / touch /
keys, headless Chrome 155 + GPU, its own profile, never signed in): `g34/b/g34-b-report.md`. Nothing on any branch,
site or bucket was changed by either.

**Result: 0 P0, 0 P1, 2 P2, 12 P3** (A 0/0/0/7, B 0/0/2/6; A's F-A5 = B-7, counted once; B-3 is owner awareness).

### What held
- **Formats (A, C1):** A's own Python decoders, written from the writers, read the PUBLISHED v2 tier 0 and all 503
  tier-1 files exactly as `decodeLines` / `decodePlaces` do (every piece, line, vertex, place, name, rank, zoom,
  population, flag: 0 mismatches). Fuzz: every truncation of a lines file throws; bad varints, a stream ending inside a
  varint, lon 180, lat 91, rank 256, an empty name, invalid UTF-8, a name-count mismatch all throw; no input hangs.
  Exceptions: F-A1, F-A2.
- **Lines per tile (A, C2):** `cellsForTile` = A's reference on 20,000 random tiles (zoom 2-18, three world copies,
  the latitude limits); `tilePaths` = A's reference (own projection, box test, dateline cut, sub-pixel skip) on 6,000
  tiles over 27 real files, 0 mismatches; another world copy gives byte-identical paths; no published line reaches
  +-180 (the date-line guard is a safety net).
- **Names (A, C3; B):** `thin` = A's O(n^2) reference of the documented rule in 4,000 random runs (0 differences;
  drawn boxes never off the map, never within 8 px of each other or 4 px of a blocked rect); `candidatesFor` = a brute
  force over copies -4..4 in 600 views; `placeShown` exact at every snapped zoom; the page's CSS = the module's
  constants. On the site (B): 98 views at six screen sizes (desktop 1280x800 and 1920x1080, tablet, three phones, a
  phone on its side): 0 overlapping names, 0 cut by the map's edge, 0 under a map control or the overlay's sheet (the
  only names under something: B-1); drawn widths = `textWidth` to 0.0 px with the default font settings (Vietnamese,
  Nepali combining marks, Greek, Hebrew, Polish, Icelandic places); Fiji the same in the copies at 180 and -180.
- **Lifecycle, store, credits (A, C4-C6):** one layout per burst of view events, hidden on zoomstart; the store's 45
  staged checks (one request set per load, the 60-s cooldown, the watchdog, <= 2 cells in flight, 404 final, the retry
  timer, `idle()`, hostile index entries never requested); credits balanced over 200 toggles and only with data.
- **Lake outlines (A, C7; B):** masks only on lake tiles at tile zoom >= 4, re-attached tiles pixel-identical, world
  copies identical, loads that resolve after an unmount / a zoom to 3 / a field change attach nothing, three syncs
  while loading attach once; wave height / period / SST tiles byte-identical with a stale wind mask on the element. On
  the site under GFS and ECMWF wind: Okeechobee, Caspian, Baikal, Victoria, Titicaca, Superior outlined on their shores.
  Exception: F-A3.
- **Page and data v2 (A, C8-C9):** the page diff as claimed (names by `textContent`, constant credits); v1 -> v2:
  134,171 records in both, exactly 235 names changed (all Natural Earth places), no other field, every lines file
  byte-identical; A's own derivation of the rule = the published change set; the pins = the published index;
  `--check` on the downloaded published set: 0 problems.
- **On the site (B):** first visit (the places index at the load event + 1.5 s, 491 KB, after the forecast); every
  settings combination kept across reloads and new tabs, the retired keys removed, phone taps, keyboard order Time Zone
  -> Units -> gridlines -> Borders -> City names -> isobars -> particles with the focus ring, the names' box aria-hidden;
  print hides the names, forced colours keep them readable; a tap or click through a name reaches the icon, forecast
  point or map under it, long-press readouts work, tooltips show above names; the (i) credits box makes the names step
  aside; six failure paths (all refused, a held index, one cell 500 retried by its timer without input, a damaged cell
  refused with the tier-0 stand-in, the lake-outline set refused, offline then online); moving the map costs nothing
  above run-to-run noise.
- **Timed playback (B, after A's DONE; GPU, 1920x1080 shark, a collected heap before play, every frame warm at the
  edge for both Origins, three Latin-square rounds of production / test / test with Borders and City names off):**
  Great Lakes z7 wind (lake outlines, borders, names, isobars) 8.0 fps in all nine runs; Europe z5 wave height 7.40 /
  7.37 / 7.37 fps on average, all three alike (the machine's slow state: render p50 76-114 ms on every variant; the
  location aids cost nothing measurable); 0 skipped, 0 unpainted; live memory within production + 10 MB (Europe +2.6,
  Great Lakes +8.2 at the maxima), peaks within production's range + 10 MB.
- Suites in A's worktree: pytest 1,295, Node 951. Console clean in every check of B's.

### Findings
- **B-6 (P2) on a slow phone connection the places download makes the overlay's coastline fail: wave height is drawn
  over the land.** The places tier 0 (491 KB) starts at the load event + 1.5 s, the same moment the overlay downloads
  its coastline (`static/coast/v1/world-i.bin`, 1.33 MB, a 15-s total budget). At 100 KB/s and 150 ms (DevTools
  throttling, phone 375x812, fresh storage, browser cache off, two runs each): production's coastline loads in
  13.6 / 13.7 s; the test site's is aborted at 15.0 s both times (the places files ran 6.6 -> 15.2 s beside it), its
  store fails for 60 s (a zoom inside the cooldown asks nothing) and wave height covers Oahu, Maui and the Big Island
  with the "Coastline data could not be loaded" note; with Borders and City names stored off before the visit the
  test site loads it in 13.8 / 13.5 s. Thresholds: production still loads it at 90 KB/s (14.9 s), the test site needs
  about 130 KB/s. So between ~90 and ~125 KB/s (0.7-1.0 Mbit/s) the release turns a clipped first view into an
  unclipped one. (Below ~90 KB/s production's own coastline already fails: the 15-s budget counts the whole download.)
- **B-1 (P2) names drawn under the floating windows and open panels, cut by their edges.** `BLOCKED_SELECTOR` holds
  the map controls, the overlay's sheet and the gridline labels only. The forecast window's bar (fixed over the map's
  bottom-left on every load, outside the map container) is not an obstacle: names fully or half under it in 13 of 35
  views at 1280x800 ("Havana" half under it at Florida z7, "Long Branch" -> "...ranch" at NYC z10, "Buenos Aires" at
  the world view), 11 of 18 at 1920x1080, 8 of 12 on a tablet (phones none: the sheet covers that band). Opened over
  the map (NYC z9, 1280x800, names fully under + cut): the gear panel 6 + 3, the tools menu 6 + 3, a live-buoy window
  55 + 5, a tide window 48 + 5, a wind window 55 + 6, the forecast window opened 65 + 9. Nothing re-lays the names when
  these open, close, move or resize (the gear panel and tools menu overflow their control without resizing its corner).
- **B-2 (P3) names over the map tools' graphics.** The names' pane (620) sits above the tools' pane (590): names are
  drawn over the swell-exposure fan and its compass letters (Rockaway z10: 7 names on the disc, "New York" on its "N";
  Ocean Beach z10: 9), and the distance tool's labels (tooltips, 650) cut the names under them ("Pear...", "Joint Base
  Pea..." across Oahu).
- **B-3 (P3, owner awareness) a town's dot looks like a wind station without a recent reading.** The names' dot is a
  4-px white dot with a dark ring; a wind station with no recent reading is an 8-px white dot with a grey ring (26 of
  the 34 wind markers at Oahu z10). Labelled and unlabelled white dots sit side by side, so a station reads as a town
  whose name was thinned, and the reverse.
- **B-4 (P3) the places store passes the plan's 10-MB budget in a long session; its cap never bites.** The LRU counts
  FILE bytes against 6 MB, but the whole tier 1 is 4.29 MB of files, so nothing is ever evicted; decoded the data is
  about 3.4x its files (Float64 positions, one string per name, a width-cache entry per measured name). 48 dense cells
  at zoom 9: test 23.1 MB vs production 10.3 MB (overlay Off, after GC); 45 surf regions: 17.6 vs 10.5. Not a leak (it
  stops at the data set's size), but a visitor who browsed ~50 cells before pressing play holds ~13 MB more than the
  memory rule's cells measured.
- **B-5 (P3) one failed tier-0 file takes both kinds away, silently.** The index and both world files load as one
  unit: with only `world-places.bin` refused the borders are gone too, both rows still ticked, until a zoom after the
  60-s cooldown.
- **B-7 = F-A5 (P3) the border layer holds a full-resolution canvas for every tile, also where there is no line, and
  stays on the map while only City names is on.** `sync()` adds the layer for either kind; `createTile` always allocates
  256 x dpr squared (1 MB at dpr 2). The default Hawaii z6 view (no line in it) holds 8 canvases / 8.4 MB on a phone,
  20 / 21 MB on a tablet, 30 / 31.5 MB at an iPad Pro size; Borders off + City names on: still 12 blank canvases
  (12.6 MB) on the phone at Europe z5. Canvas backing stores are outside the heap the memory rule measures.
- **B-8 (P3) with a browser minimum font size the names overlap and can run off the map.** Widths are measured on a
  canvas at the fixed `LABEL_FONT` (11 px), which a minimum font size does not touch, while the text renders at the
  minimum: with Chrome's "Minimum font size" 14 px every name is 12-47 px wider than its box; overlapping names at Oahu
  z10, US east z6, NYC z10 (9 pairs), Europe z5, Tokyo z10; on a phone "Dresden" ends past the right edge. (Minimum
  font sizes are supported since section 43.)
- **F-A1 (P3) `decodeLines` allocates by the header before it validates the stream.** A 60-byte file whose header
  claims 5 million pieces / lines / vertices makes it allocate ~277 MB before throwing. Only a compromised origin could
  serve it (immutable objects over HTTPS), but on a phone it is a tab crash. `decodePlaces` already checks first.
- **F-A2 (P3) a places file cut inside its last name decodes; no file's length is compared with the index.** 9 of
  3,176 truncations of a cell and 19 of the last 20 lengths of `world-places.bin` return a shortened last name;
  single-bit damage inside a lines file usually decodes to shifted points (a delta format without a checksum). The
  index gives every file's byte count and the client never compares it.
- **F-A3 (P3) after the lake-outline set is let go the masks live on through the edge cache.** The detach branch of
  `setLakeOutlineCoast` clears `_ovLo` / `_ovLoKey` but not `_ovLoEdges` / `_ovLoEdgesOf` (= the 64-KB mask): Great
  Lakes z6 under wave height keeps 11-21 masks (0.7-1.3 MB) until the tiles go. (The author's and both earlier checks'
  "0 masks" counted `_ovLo` only.)
- **F-A4 (P3) the ResizeObserver never lets a replaced overlay sheet go.** Every phone Off -> On or compact switch
  re-creates the sheet; each old one stays observed (50 replacements -> 50 observed elements; a few KB each).
- **F-A6 (P3, wording) the tier-0 "15-s stall" is one 15-s budget for the three files together**: a visitor slower
  than ~35 KB/s never gets borders or names (each try aborts at 15 s, retried only by a zoom after 60 s).
- **F-A7 (P3 nit) the places test fixtures are v1 crops:** `world-places-crop.bin` holds 10 names v2 renamed; its
  README says places-v1 (the cell and lake fixtures are byte-identical to v1 and v2).
- **Mutant survivors worth pinning (A: 82 mutants, 66 killed, 16 survived; none hides a shipped defect):** M25
  `store.idle()` not called when both kinds go off; M40 the call site's 3-px tile margin; B04 a case-sensitive edit
  distance in the name pass (the real build would lose 5 fixes, e.g. Tarin Kowt -> Tarinkot, and the 220-250 gate would
  not notice); B11 / B12 `NAME_FIX_RANGE` and `NAME_PROBES` unpinned; B06 the name pass moved after the dedupe. The other
  10 are equivalent or cost-only (A's report lists each).
- **Observations, no action:** the "236 places" of owner decision 10 vs the index's 234 agreed is two "Granada" names
  carrying an invisible left-to-right mark that `clean_name` strips (for the builder NAME == NAME_EN already: 234 + the
  hand fix = 235); three GeoNames towns sit at a tier-0 place's exact position with another name (the thinning keeps the
  larger, same dot); credits follow the rows and the data, not the view; dense metro views fill the 120 cap; the lines
  are subtle by design; K-4 reproduced (a pan never retries a failed load).

### Author's verification
- F-A1, F-A2 and F-A3 confirmed in the code: `decodeLines` sizes six arrays from the header's counts and checks the
  stream's length only at the end; nothing compares a body's length with the index's byte counts; the detach branch
  nulls `_ovLo` and `_ovLoKey` only.
- F-A4 (`watchSheets` marks and observes, nothing unobserves), B-7 / F-A5 (`sync()` adds the layer for either kind,
  `createTile` always allocates), B-4 (`MAX_CELL_BYTES_KEPT` 6 MB of file bytes against 4.29 MB for the whole tier 1),
  B-8 (`measureText` with the fixed `LABEL_FONT` / `LABEL_H`) and B-1 (`BLOCKED_SELECTOR` = controls, sheet, gridline
  labels) confirmed in the code.
- B-6: the mechanism is in B's request timings (the coastline download and the two places files running together from
  6.5 s, the coastline cancelled at its 15-s budget in both test runs, loaded in both production runs and both aids-off
  runs); not re-run.

### Owner's calls (2026-10-23, AskUserQuestion, all recommended)
- **B-6 slow links: "Wait + patient coastline".** The borders and names wait until the overlay's coastline and first
  picture have landed or failed (at most ~30 s) and download at low priority; the coastline's 15-s limit counts only
  time with no data arriving (also helps links slower than production's own ~0.7 Mbit/s limit).
- **B-2 map tools: "Names avoid the tool".** While a tool shows its fan or its labels, the names are laid out around
  them.
- **B-3 dots: "Grey out no-reading stations".** A wind station without a recent reading becomes a hollow grey ring;
  the town dots stay. The owner sees it on the test site before it ships.

### Fix round scope (step 9; UI 1.25.2, overlay 2.22.2)
1. **B-6:** the places start after the window's load + 1.5 s AND the overlay's coastline tier 0 and first picture
   (landed or failed), capped at 30 s; with the overlay Off or not mounting, the load + 1.5 s rule as now; the places
   files fetched with `priority: 'low'` where supported. The overlay's coast store reads its tier-0 file and index with a
   stall timer (15 s with no bytes, not 15 s in total) where `r.body.getReader` exists (the SST store's pattern); the
   places store's tier-0 watchdog the same (F-A6).
2. **B-1:** the visible floating windows (`.fwin:not([hidden])`, their bars and chips) and the open gear and tools panels
   become blocked rects, queried from the document and clipped to the map; the names are laid out again when one opens,
   closes, moves or resizes (a ResizeObserver on each, a MutationObserver on their class / style / hidden).
3. **B-2:** the tools' fan (`.tools-fan`) and their labels (`.tools-label`) become blocked rects while shown; the names
   are laid out again when the tools pane's children change.
4. **B-3:** `.wind-none` draws a hollow grey ring (template CSS: the golden re-captured in its own commit).
5. **B-4:** the cell LRU counts decoded bytes (typed arrays + 2 x the name characters) against 6 MB and never evicts a
   cell the view needs; the width cache goes with its cell.
6. **B-5:** the tier-0 lines and places load as two units (a names failure keeps the borders, and the reverse), each
   with its own credit.
7. **B-7 / F-A5:** the line layer on the map only while Borders is on; a 1x1 canvas for a tile without a path.
8. **B-8:** widths measured with the rendered font (a hidden `.place-label` probe's computed font and line height, and a
   capital's, read at start and on resize).
9. **F-A1** the count check before any allocation; **F-A2** every body's length compared with the index's byte count (a
   cell: final; tier 0: a failed load); **F-A3** the detach also clears `_ovLoEdges` / `_ovLoEdgesOf`; **F-A4** removed
   sheets unobserved; **F-A6** the wording; **F-A7** the places fixtures re-cropped from v2, their README.
10. Pins for the survivors M25, M40 (places tests) and B04, B11, B12, B06 (builder tests only; the builder code on
    production unchanged).
11. Then the test site at MAX (B's slow-link runs at 100 / 90 / 60 KB/s, test against production; every fix live; the
    owner's look at the grey ring), a short fresh re-check at MAX (the fixes touch the overlay's coastline loader and the
    places store), then step 10.

## Fix round (step 9, 2026-10-23, Opus 5.5 HIGH)
`feat/location-aids` @ fb78252 (code + tests), 0330f9d (the golden, its own commit), then the survivor pins and this
section. UI asset 1.25.2, overlay asset 2.22.2. Not on the test site yet.
- **B-6** (owner: wait + patient coastline): `Overlay.settled()` resolves once the first mount has drawn a picture or
  failed, or the overlay went Off, after the coastline's tier 0 has landed or been given up; the gated block keeps it
  as `window.__allshoreOverlayWait` (a promise only while a layer is restored or picked; settled at once by Off or an
  asset failure). `AllshorePlaces.create(map, {waitFor})` starts its downloads after the load + 1.5 s AND that promise,
  at most 30 s (`afterWait`, `WAIT_MAX_MS`). Every places request asks for `priority: 'low'`. The overlay's coast store
  (`CoastStore.load`) and the places store give a download up only after 15 s with no byte (`readBody` with a cap and
  a message / `readAll`, each chunk re-arming the timer), not after 15 s in all (also F-A6).
- **B-1**: `.fwin` windows (queried from the document, hidden ones skipped) and `.settings-panel` (the gear and tools
  menus) are blocked rects; a ResizeObserver on each and a MutationObserver on their class / style / hidden lay the
  names out again.
- **B-2** (owner: names avoid the tool): `.tools-fan`, `.tools-label` and `.tools-ring-label` are blocked rects; a
  subtree childList observer on the map container schedules a layout only when such a node (or an overlay sheet) comes
  or goes (a tile or a name of its own never does).
- **B-3** (owner): `.wind-none .wind-ring` is a hollow grey ring (border 2 px #b8c1cb with a dark outline inside and out,
  same 10-px box and tap target).
- **B-4**: the LRU counts decoded bytes (`decodedBytes`: typed arrays + 2 x the name characters + 32 a name) against
  6 MB; a cell used by the current view (`store.view`, advanced on movestart / zoomstart / viewreset) is never evicted;
  the width cache starts again past 8,000 entries. (Deviation from the scope's "the width cache goes with its cell": a
  size cap does the same for memory without tracking which cell a name came from.)
- **B-5**: `PlacesStore.unit.l` / `unit.p`: the tier-0 lines and places load as two units after the index; `ok(kind)`
  gates the drawing and each credit; a failed unit is asked again after its cooldown without the index.
- **B-7 / F-A5**: the line layer is on the map only while Borders is on; a tile without a line keeps a 1x1 backing
  store, grown when a line comes into it.
- **B-8**: a hidden probe of each label kind gives the computed font and line height (read at the first layout and
  again after a map resize); the widths and `thin`'s box height (new 5th argument) follow it.
- **F-A1** the count check before any allocation; **F-A2** every file's length against the index (a cell: final; a
  tier-0 file: its unit fails); **F-A3** the lake-outline detach also clears `_ovLoEdges` / `_ovLoEdgesOf`; **F-A4** a
  removed sheet is unobserved; **F-A7** `world-places-crop.bin` re-cropped from the published v2 file (the same 494
  records, the corrected names), `places_expect.json` and the README with it.
- Pins: M25 (both off stops the downloads), M40 (a line within 3 px of a tile is drawn), B04 (the case-folded edit
  distance: Tarin Kowt -> Tarinkot), B11 / B12 (`NAME_FIX_RANGE`, `NAME_PROBES`), B06 (the dedupe sees the corrected
  names).
- Tests: pytest 1,298 passed; Node 962 + the survivor pins (places 37, coast 44 tests in their files). Mutants of the
  fix round (scratch `g34/fix/mut9.py`, on a copy): 33, all killed after four pins (a move's new view, the credits per
  unit, the render and Off notes of the first picture).

## Test-site verification of the G34 fixes (2026-10-23/24, Opus 5.5 MAX)
`test` @ 5f5cf12 (cherry-picks of fb78252, 0330f9d and 431a139's test pins; README and this record kept off the test branch;
the site files = the feature branch's except the three test-only logo lines), pushed 23:43:34 UTC, live from 23:45:01 UTC. 30 polls across the switch, 0 failed. Served `places.js` / `forecast.js` / `tools.js` / `graticule.js`
(UI 1.25.2) and `overlay.js` / `overlay.css` (2.22.2) = the commit; the 1.25.1 / 2.22.1 URLs answer 404. Headless Chrome
with the GPU (scratch `g34/v9`: reviewer B's harness on port 9771, profile prefix `chromeprof-v9-`, B's scripts with a
storage clear at the start of each run; outputs in `out/`). Production (2.21.7 / UI 1.24.0, no location aids) measured in
the same runs where it compares.

### B-6: slow links (`s/17_slow.js`, throttled by DevTools, 150-ms latency, a fresh visit each)
| link | test 2.22.2 | production 2.21.7 |
|---|---|---|
| 100 KB/s | coastline in 13.8 s (6.6 -> 20.4 s), clip on; places index at 21.4 s (after the coastline and the first picture); places file in at 27.6 s, lines file at 32.2 s; the name on screen by 35 s | coastline in 13.6 s, clip on |
| 90 KB/s | coastline in 15.1 s (7.4 -> 22.5 s: past 15 s in total, kept by the no-progress rule), clip on; places from 23.7 s | coastline cancelled at 15 s (`ERR_ABORTED`), "Coastline data could not be loaded", wave height over the land |
| 60 KB/s | coastline in 22.2 s (10.4 -> 32.6 s), clip on; places from 33.9 s | cancelled at 15 s, the same warning |
| 30 KB/s | the coastline still loading at the 30-s cap (load 20.3 s + 1.5 s + 30 s): the places index asked at 51.8 s, the places file in at 66.7 s, the lines file and the coastline still loading at 76 s; both complete by ~84 s, the coastline clipped, the names drawn | (not run: fails from 90 KB/s) |
The test site keeps its coastline on every link production loses it on; the places never take the line before the
overlay's coastline and first picture, except past the 30-s cap.

### B-1: names clear of the floating windows and the open panels
- `s/05_windows.js` at 1280x800 (trusted clicks on a live buoy, a tide station, a wind station and the forecast chip; the
  gear and tools menus opened): names under something: 0 in every state (start 109 names, gear 104, tools menu 106, live
  window 54, tide window 64, wind window 53, the forecast window open 43). B before: gear 6 + 3, tools 6 + 3, live 55 + 5,
  tide 48 + 5, wind 55 + 6, forecast 65 + 9 (covered + partial). Console clean.
- `s/02_views.js` with the storage cleared (B's 05 run had left the forecast window open in that tab: names under it
  are left out, as designed) at 1280x800, 1920x1080, a 1024x768 tablet and the 375x812 phone: 0 overlapping pairs, 0 cut,
  0 covered, 0 partial in every view. The names B found covered are the ones left out (e.g. Havana, Myrtle Beach, Buenos
  Aires, Johannesburg, São Paulo, Petropavlovsk-Kamchatsky); totals 1,075 -> 1,050 (1280), 996 -> 982 (1080), 437 -> 415
  (tablet), 188 = 188 (phone); a freed place sometimes takes another name (Tokyo z10: Kawara, Kotsubaki); views B had
  clean give the same names.
- Layout churn: 0 label writes over 10 s at rest and over 8 s of shark playback (51 frames).
- The names come back (`s/v9_return.js`, 1280x800, trusted clicks and keys, the map never moved): Europe z5 53 names ->
  the gear panel open 50 (Chișinău, Kyiv, Sevastopol left out) -> closed 53, the same set; the tools menu open 52 ->
  Escape 53; San Francisco z10 66 -> the swell-exposure fan and bar 43 (three other names find room) -> the tool's x 66,
  the same set; New York z9 107 -> a live-buoy window (44025) 57 -> Escape 107; the forecast window expanded 45 ->
  minimised 107, the same set each time; 0 overlaps / cut / covered / partial in every state; console clean.

### B-2: names avoid the map tools (`s/08_tools.js`, 1280x800)
Swell exposure at San Francisco, Haleiwa and New York (fan, its labels, the range rings' labels, and hovering a wedge),
Measure distance, Measure area and a forecast point: 0 names overlapping any tool element (B: 1 to 9 per state).

### B-3: the grey no-reading ring (`s/v9_ring.js`)
`.wind-none .wind-ring` on the test site: transparent, `2px solid rgb(184, 193, 203)` with dark 1-px outlines inside and
out, the same 10-px box; production: a white 8-px dot with a 1-px grey edge. Oahu z10: 27 of 36 wind markers are
no-reading rings (Honolulu z11 44 of 51, Maui z10 40 of 43). A trusted click (desktop) on Kaluanui Ridge's ring and a
tap (phone) on Lyon's open their wind windows. Sheet for the owner: scratch `g34/v9/shots/g34_fix_grey_ring.jpg`.

### Failure paths (`s/10_fail.js`, DevTools interception)
- Every places file refused on a first visit: no lines, no names, no credits, rows kept; allowed again, nothing asked
  inside the 60-s cooldown; the first view change after it draws both (44,029 line px, 54 names, both credits).
- The index held 20 s: the watchdog fails it at 15 s; a late answer is ignored.
- B-5: `world-places.bin` refused while the index and `world-lines.bin` answer: units `ok/failed`, the borders drawn and
  credited, no names, no places credit.
- A cell answering 500 (Biscay z9, `40_0.p`): 7 names from tier 0 and the other cells; 62 s later, with no input, the
  retry timer brings it (45 names). A cell answering 200 with damaged bytes (`40_0.l`): final, tier 0 drawn instead, no
  console noise, never asked again.
- The lake-outline set refused under wind: no outlines; 62 s later + a zoom: attached, 21 tile masks.
- Offline after the data loaded, then browsing at zoom 9 (Rome): tier 0 only (3 names), the two cells in their
  cooldown; online again: 65 s later, with no input, both cells landed (66 names).
- 2b (`world-places.bin` held 8 s) does not discriminate here: the harness's settle waits for the store.

### Load order on a normal link (`s/v9_order.js`, fresh visits)
| case | load | coastline | first frame | settled() | places index |
|---|---|---|---|---|---|
| desktop 1280x800 | 414 ms | 745-754 | 1,105-1,112 | 1,246 ('ready') | 1,920 (load + 1,506) |
| phone 375x812 | 400 | 663-678 | 854-864 | 990 ('ready') | 1,903 (load + 1,503) |
| overlay saved Off | 437 | - | - | - | 1,950 (load + 1,513, nothing to wait for) |
On a normal link the wait costs nothing: the overlay settles before the 1.5-s idle ends.

### B-4: memory after a long browse (`s/12_memory.js`, overlay Off, JS heap + array buffers after a GC)
| browse | test | production | difference | B on 2.22.1 |
|---|---|---|---|---|
| dense (48 dense views, Europe / US / Japan at z9-10) | 19.5 MB (store: 74 cells, 5.94 MB decoded, tier 0 2.75 MB) | 10.4 MB | +9.1 MB | +12.8 MB |
| surf spots (45 views) | 17.0 MB (78 cells, 3.22 MB decoded) | 10.5 MB | +6.5 MB | +7.1 MB |
The store's count stays under its 6-MB cap (decoded bytes) and the cells of the view are kept.

### Model tiles = production (`s/b6_identity.js`, the same run 2026102318, valid 2026-10-25 06Z, lake run 2026102319)
Wave height, peak period and SST at the Great Lakes z6 and Okeechobee z8: 144 tiles, 0 different from production.
Wind (192 tiles in all): differs only on lake edges (the lake outlines; 51,086 + 2,630 px, 0 off a lake edge, 21 + 4 tiles
with a mask). Console clean.

### B-7 / F-A5 and F-A3: canvases and lake masks (`s/18b_canvas.js`, `s/18_confirm.js`)
Backing memory of the border-line layer (B on 2.22.1 -> now):
| device | Hawaii z6 (the default view: no line) | Europe z5 |
|---|---|---|
| phone 375x812 dpr 2 | 8.4 MB -> 0 (8 canvases of 1x1) | 12.6 -> 11.5 MB (11 tiles with a line) |
| tablet 1024x768 dpr 2 | 21 -> 0 | 21 -> 15.7 (15) |
| iPad Pro 1366x1024 dpr 2 | 31.5 -> 0 | 31.5 -> 26.2 (25) |
| desktop 1920x1080 dpr 1 | 12.6 -> 0 | 11.8 -> 7.9 (30) |
Borders off with City names on: 12.6 MB -> no line layer on the map (0 canvases); map zoom 2.2 (no tile draws a line):
6.3 MB -> six 1x1 canvases. F-A3: after wind -> wave height at the Great Lakes z6 the 21 tiles' edge caches held their
masks (1,344 KB) on 2.22.1; now 0.

### B-8: minimum font size (`hl/hl_start_minfont.js` + `s/16_minfont.js`, a 14-px minimum in the browser's settings)
The names render at 14 px and are now measured at 14 px: the widest name's DOM width minus the module's measured width is
0 px in every view (B on 2.22.1: 12.6 to 46.9 px). 1280x800, seven views (Hawaii z6, Oahu z10, US east z6, New York z10,
Europe z5, Tokyo z10, world z3): 342 names, 0 overlapping pairs (B: 13), 0 cut, 0 under something (B: 17). Phone
375x812: 91 names, 0 overlapping (B: 8), 0 cut (B: 1), 0 under something (B: 1). Fewer names where they are wider, as
designed. Console clean.

### Timed playback (`w54p.js`, PREGC, frames warm at the edge for both Origins, GPU)
Run 2026102318, warmed for both Origins (1,463 requests each), PREGC, ABBA per cell, 20-s idles, 1920x1080:
| cell | production 2.21.7 | test 2.22.2 |
|---|---|---|
| Europe z5 wave height shark | 8.0 fps (gap p50/p90 125/135 ms); 6.8 (125/135, one 4.5-s stall in production's own run) | 8.0 (125/134); 8.0 (126/135) |
| Great Lakes z7 wind shark | 8.0 (125/134); 7.7 (125/135, max 596) | 8.0 (126/135); 7.7 (125/136, max 708) |
The second pair of the wind cell shared one slow stretch at the edge (edge answers up to 1.35 / 1.46 s in both runs).
0 skipped and 0 unpainted landings in every run; frames fetched once each (1.02 per landing); in flight never over the
cap. Memory (page heap + array buffers vs Off): Europe live +39.1 / +39.1 MB vs production +39.2 / +39.1, peaks +60.2 /
+60.4 vs +60.6 / +60.1; Great Lakes wind live +36.6 / +36.3 vs +33.4 / +33.2 (+3.2 MB: the lake outlines' masks at
zoom 7, new in this release), peaks +62.6 / +69.8 vs +60.5 / +60.3: inside the owner's rule (live <= production + 10,
peaks <= production's range + 10). Main thread: Europe 772 / 779 ms/s vs production 768 (and 674 in its stalled run);
wind 611 / 632 vs 607 / 619. The test page's starting heap is 3.1 MB larger (the places' tier 0, decoded).

### Console smoke (a fresh page target)
A new page target (no log replayed from earlier pages), the storage empty, at 1280x800: the first visit (UI 1.25.2,
wave height restored, 2 names at the default view), Europe z5, Biscay z9, Oahu z11, the world at z3, wind, the Great
Lakes z6 under wind, SST, Off, wave height again, the gear's Borders and City names off and on (trusted clicks), the
forecast window expanded and minimised, the swell-exposure tool at San Francisco and its x, a reload (66 names at the
saved view): 0 console errors, warnings or exceptions. The harness browser stopped, its profile removed, no process
left; the storage of the test and production origins emptied; the in-app browser pane was not used.

### Findings
Every G34 item holds on the test site, and nothing measured is worse than production. One new P3:
- **V9-1 (P3): on a slow link the names wait for the borders file.** `PlacesStore.load` resolves after BOTH tier-0 units
  have landed or failed (`Promise.all`), and the page draws once per load ("one redraw per load", `sync()`): a unit that
  lands first draws nothing. The places file (144 KB) usually lands first and the names then wait for the lines file
  (342 KB): measured in `s/17_slow.js`, places in -> lines in = 27.6 -> 32.2 s at 100 KB/s, 30.8 -> 36.3 s at 90, 44.6 ->
  53.9 s at 60, 66.7 -> ~84 s at 30 (the names appeared only after the lines); a stalled lines file holds the names up to
  the 15-s no-progress limit. On a normal link both land within ~10 ms (`s/v9_order.js`: 1,924-1,931 and 1,925-1,930 ms),
  so nothing changes there. B-5 made a FAILED unit independent; a SLOW one still holds the other. Fix (small): the unit's
  success branch tells the page (`onChange`) when the other unit is still loading, so each kind draws as it lands (one
  extra redraw, slow links only); a test with one file held.
Owner awareness (no change proposed):
- Where a town and a no-reading wind station share a spot, the town's dot sits inside the grey ring (Kēōkea, Maui z10:
  it reads as a target symbol). It follows from names drawn above the station icons (owner decision D, step 7).
- With a floating window or a panel open, the names under it are left out and others take the free room. The set
  returns exactly when it closes.
- On links slower than about 40 KB/s the 30-s cap starts the places while the overlay's coastline is still loading; both
  still complete (measured at 30 KB/s: the coastline and the names by ~84 s).

## Re-check of the fix round (2026-10-24, Opus 5.5 MAX, one fresh reviewer)
One fresh-context reviewer (code `git diff 44ce96a..431a139` + the test site @ 5f5cf12 with trusted input; brief and
report in scratch `g34/recheck/`). **0 P0, 0 P1, 1 P2, 5 P3.**

### What held
- C1 the wait chain in every path (code read; live on a normal link: SST, ECMWF wave height and wind restored, a field no
  longer offered, Off saved, a switch to period and Off during the wait): the places' index at load + 1,502-1,512 ms every
  time; the 30-s cap fires at load + 31.5 s; `settleFirst` idempotent.
- C2: a coastline with no byte for 15 s is still given up (16.1 s, as production); places requests at priority Low, the
  overlay's at High.
- C3 / C4: 35 views at 1920x1080 and 35 on the phone, 0 overlapping / cut / covered / partial names; the forecast window
  expanded, resized and minimised: none under it, the same set back; phone playback (sheet up, shark, wave height and
  wind): 0 label writes, 0 ResizeObserver callbacks.
- C5 the ring's look; C6 one 24,000-px drag at Paris z9: 9 cells, 2.1 MB decoded, the final view's cells in 0.5 s; C9 at a
  24-px minimum font: widths exact, 0 glyph overlaps; C10 F-A1 fuzz (4,000 hostile headers refused in < 0.35 ms), F-A2
  truncations refused, F-A3 / F-A4 live; C11 pytest 1,298 and Node 964 / 964.

### Findings
- **R-1 (P2) on a slow link the first wave picture waits for the whole coastline.** `mount` requests the first frame only
  after `coastP` resolves (overlay.js ~4021-4045; `_mountObs` likewise); before G34 the coastline was given up after 15 s in
  all and the picture came unclipped with the warning; with the patient coastline it comes after the whole 1.33 MB, and
  never while the file trickles. Below ~44 KB/s the places' 30-s cap starts the places on the same link. First wave
  picture, phone, DevTools throttling with 150-ms latency, test (clipped) / production (unclipped + warning): 60 KB/s 34.0 /
  26.7 s (+7.3); 45 KB/s 48.8, 49.1 / 30.5, 30.1 s (+18.6); 30 KB/s 82.9, 83.0 / 37.3, 37.2 s (+45.7; the cap costs ~17 s,
  the names before the picture); the coastline alone at 150 B/s: no picture at 121 s / 16.1 s. Caveat: DevTools throttling
  ignores request priority.
- **R-2 (P3) the author's V9-1 fix is not enough, and the wait reaches Borders-off users.** `update()` sets
  `labelsComplete = true` while the places are not usable (places.js ~623), so an `onChange` per unit lays nothing out;
  with the places file held it would draw borders without their credit; it breaks the pin at tests/ui/places.test.js ~336.
  Live: world-lines.bin at 2 KB/s held the names for 150 s although the places file landed at 4.7 s; with Borders OFF the
  lines file is still downloaded (`need` ignores which kinds are on, places.js ~249) and still holds the names.
- **R-3 (P3, owner awareness) a no-reading ring looks the same hollow or not.** `.wind-none .wind-ring` (index.html ~533)
  overrides the hollow look; `.wind-hollow.wind-none` keeps `pointer-events: none`: one ring opens the wind window, an
  identical one beside a forecast point or a My point passes the tap to the dot (G28's rule). Rare (72 no-reading
  stations, 9 hollow, 0 both in six views).
- **R-4 (P3, pre-existing) the coastline's tier-0 download is not stopped when its index fails** (overlay.js ~734-764: the
  catch clears the watchdog and `loadAbort` but never aborts; each chunk re-arms the timer; Off cannot stop it).
- **R-5 (P3, pre-existing) a cell with invalid UTF-8 names is asked for again every 60 s** (the TextDecoder's TypeError
  does not match the final-failure pattern, places.js ~351).
- **R-6 (P3, tests) 26 of the reviewer's 49 mutants survive, 15 of them real gaps:** R33 the page never retrying a failed
  unit; R19 / R20 / R21 the font probe after a resize, its line height, the width cache on a font change; R27 the panels
  not watched; R12 a cell's stall timer not re-armed by its chunks; R7 / R8 / R5 the streamed byte caps; R2 the 1.5-s idle;
  R39 `settleFirst` on an asset failure (survives pytest too); R43 / R44 the 15-s constants; R16 / R18 weakly asserted.
- Outside the fix round (noted): two timing-sensitive tests (an SST store test, fan placement) failed once each under CPU
  load; at a 24-px minimum font the overlay's own timeline ribbon is garbled (section 43's area).

### Author's verification
- R-1: confirmed in the code: `(waitCoast ? coastP : ...)` sits between the manifest and `_goto`, so the frame is not even
  requested until the coastline lands; the author's own 17_slow data agree (30 KB/s: no picture at 76 s).
- R-2: confirmed (`update()`'s early return sets `labelsComplete = true`; `onChange` schedules only when it is false;
  `need` filters by status only). R-3: confirmed in the CSS (specificity: the later `.wind-none .wind-ring` sets the box,
  border and shadow; `.wind-hollow.wind-none .wind-ring` only the background and pointer-events). R-4 and R-5: confirmed
  in the code.

### Owner's calls (2026-10-24, AskUserQuestion)
- **R-1: "Draw at 15 s, clip later" (recommended).** The waves appear after at most ~15 s without the coast clip and with
  a "Coastline still loading" note; the coastline keeps downloading and the picture is clipped the moment it lands; the
  first frame downloads beside the coastline; the city names wait until the coastline is done or given up (at most 120 s).
- **R-3: "Keep as is".** Both no-reading rings look the same (rare); recorded as owner awareness.

### Fix round 2 scope (UI 1.25.3, overlay 2.22.3)
1. **R-1:** in `mount` (and `_mountObs`) the first frame is requested as soon as the manifest is in; the first draw waits
   for the coastline's tier 0 at most 15 s from the mount, then draws unclipped with the note; the coastline lands ->
   `setCoast` + one redraw + the attribution, the note goes; it fails -> today's warning. The download stays patient (15 s
   with no byte gives it up). `settled()` resolves when the first picture is drawn (or failed / Off) AND the coastline has
   landed or been given up; the places' cap becomes 120 s. Tests with fake timers (the 15-s draw, the clip on landing, a
   failure after the unclipped draw, a superseded mount, Off, a model / field switch, SST, settled's order, the cap).
2. **R-2:** only the units of the kinds that are on are loaded (turning a kind on later loads its unit); each unit draws
   and credits its own kind as it lands (names laid out even when the last layout found the places unusable), not while
   a zoom animates; the pin at tests/ui/places.test.js ~336 updated; tests with each file held, both orders, Borders off.
3. **R-4:** the catch aborts its own controller; `arm` does nothing once the load has settled; a test.
4. **R-5:** `decodePlaces` turns a TextDecoder error into `places-v1: names are not UTF-8` (final); a test.
5. **R-6:** pins for R33, R19, R20, R21, R27, R12, R7, R8, R5, R2, R39, R43, R44, R16, R18.
6. Versions + fixtures; the golden only if the template changes (its own commit); README (the slow-link behaviour).
7. Then the test site at MAX (the K-2 runs re-measured against production at 60 / 45 / 30 KB/s and the trickle; R-2 live:
   one file held, Borders off), a short fresh check at MAX (the first-draw path changed), then step 10.

## Fix round 2 (2026-10-24, Opus 5.5 HIGH)
`feat/location-aids` @ e8f8eb5 (code + tests), df3150f (the golden, its own commit: the version strings only), then this
section. UI asset 1.25.3, overlay asset 2.22.3. Not on the test site yet.
- **R-1** (owner: "Draw at 15 s, clip later"): `mount` computes the first index as soon as the manifest is in and asks for
  that frame at once, beside the coastline; the first draw of wave height / period / SST (also `_mountObs`) waits for the
  coastline at most `COAST_FIRST_MS` (15 s; `_coastFirst`), then draws unclipped and the panel says "Coastline still
  loading; the field is shown without coastline clipping until it arrives." (`coastNote`); `_coastLate` clips whatever
  clipped or outlined field is on the map the moment the coastline lands and re-renders the panel (the note goes, or turns
  into "could not be loaded"). `settled()` already waited for the coastline (`_noteSettled`), so the places still never
  take its line; their cap is now 120 s (`WAIT_MAX_MS`). **Found while wiring it:** a frame's watchdog limits the WHOLE
  download to 5 s (section 41's late frames); sharing a slow link with the coastline, a desktop's full-resolution first
  frame (169 KB) would have been cut below ~68 KB/s (alone: ~34 KB/s, already the case on production). The first frame of a
  mount (nothing on the map) is now given up only after 5 s with no byte arriving (the chunks re-arm its watchdog); every
  later frame keeps the total limit.
- **R-2**: `PlacesStore.load(kinds)` fetches only the tier-0 files of the rows that are on (Borders off: no lines file); a
  unit that lands calls `onUnit(kind)`: the page credits it and draws that kind at once (lines: a redraw; names: a layout,
  not during a zoom, whose end lays them out); `update()` keeps `labelsComplete` false while the places file is loading;
  a kind turned on later (or during a load) fetches its file.
- **R-4**: a coastline load that fails aborts its own controller after the status is read (the tier-0 download stops when
  its index fails); its timer is not re-armed once the load has settled.
- **R-5**: `decodePlaces` turns a TextDecoder error into `places-v1: names are not UTF-8` (final, never asked again).
- **R-6**: pins for R2 (the 1.5-s idle with nothing to wait for), R5 (the coastline's own byte cap), R7 / R8 (`readAll`'s cap
  and cancel), R12 (a cell's chunks re-arm its stall timer), R16 (each name's bytes), R18 (the width cache starts again),
  R19 / R20 / R21 (a re-probe after a resize, the probe's line height, widths dropped on a new font), R27 (the panels
  watched), R33 (a failed unit asked again after its cooldown), R39 (the overlay script failing settles the wait), R43 /
  R44 (the 15-s constants). The module returns `fontUsed()` for the probe's pins.
- Tests: Node 979 (+16), pytest 1,298. Mutants (scratch `g34/fix2/mut_fix2.py`, on a copy): the reviewer's 15 real survivors
  re-applied (R33 re-anchored on the new `sync()`): 15 killed; 19 of the fix round's own: 18 killed after one more test (a
  kind turned on during a load); F16 (`labelsComplete` true while the places load) is equivalent: the unit's own landing
  lays the names out.

## Test-site verification of fix round 2 (2026-10-24, Opus 5.5 MAX)
`test` @ d316c15 (cherry-picks of e8f8eb5 as 5ce723c, the golden df3150f as d25f8de, and 99a314f's test as d316c15; README and
this record kept off the test branch; the site files = the feature branch's except the three test-only logo lines), pushed
03:33:01 UTC, live from 03:34:16 UTC. The first poll (03:33:06) timed out: the free test instance was waking from idle, and
the next poll (03:33:31) was answered by the old instance; across the switch itself (03:34:10 old -> 03:34:16 new) 0 failed.
Served `places.js` / `forecast.js` / `tools.js` / `graticule.js` / `basemap.js` (UI 1.25.3) and `overlay.js` / `overlay.css`
(2.22.3) = the commit; the 1.25.2 / 2.22.2 URLs answer 404; the served code carries `COAST_FIRST_MS = 15000`,
`STALL_MS = 5000`, `WAIT_MAX_MS = 120000`. Headless Chrome with the GPU (scratch `g34/fix2v`: the re-check reviewer's harness
on port 9791, profile prefix `chromeprof-f2v-`; `s/k2_first.js` with the note / clip-landing fields added, `s/k2x.js` its
variant with the desktop, the field and the frame counts, `s/r2_live.js`, the v9 regression scripts; outputs in `out/`).
Production (overlay 2.21.7 / UI 1.24.0, no location aids) measured in the same runs, run 2026102318 on both.

### R-1: the first picture on a slow link (`s/k2_first.js`, `run_k2.sh`; phone 375x812 dpr 2, cache off, storage empty = wave height restored, DevTools throttling with 150-ms latency, ABBA order)
| link | test 2.22.3: first picture (unclipped, "Coastline still loading") -> clipped | production 2.21.7 (unclipped, "could not be loaded", never clipped) | 2.22.2 (re-check: the clipped picture) |
|---|---|---|---|
| 60 KB/s | 26.3 / 26.3 s -> 46.1 / 46.3 s | 28.5 / 26.4 s | 34.0 s |
| 45 KB/s | 29.6 / 30.6 s -> 69.6 / 69.4 s | 29.9 / 30.0 s | 48.8 / 49.1 s |
| 30 KB/s | 36.0 / 36.3 s -> 76.5 / 76.8 s | 37.3 / 37.1 s | 82.9 / 83.0 s |
| the coastline alone trickling at 150 B/s (the rest fast) | 16.6 s; the note stays (the coastline would take ~2.5 h); the names at 122.6 s (load 0.8 s + 1.5 s + the 120-s cap) | 15.8 s | none in 121 s |
The first picture is now within ~1.5 s of production's at every speed (it was 7-46 s later), and the test site still keeps the
coastline: the note goes and the field is clipped when the coastline lands. The first frame is asked right after the
manifest, beside the coastline (60 KB/s: the coastline at 10.4 s, the frame at 11.2 s, landed 13.0 s, drawn at 26.3 s = the
15-s wait counted from the manifest). The places still wait for the overlay's `settled()` (the coastline): their index is asked at
the clip (within 0.1 s) in every run. The clip lands later than 2.22.2's clipped picture at 60 and 45 KB/s (46 vs 34 s,
69.5 vs 49 s): after the first picture the ring (its field and direction frames) downloads beside the coastline (see V2-1).

### The note and the clip on landing, by field (`s/k2x.js`, the coastline alone at 50 KB/s: it lands after the 15-s wait, the rest fast)
| field | test 2.22.3 | production 2.21.7 |
|---|---|---|
| wave height (phone) | drawn at 16.3 s unclipped with the note; clipped and the note gone at 27.1 s; settled 27.1 s, the names 27.3 s | drawn at 15.6 s unclipped, "could not be loaded" |
| wave height (desktop 1280x800) | 16.5 s -> clipped 27.3 s | (not run) |
| peak period | 16.4 s -> 27.2 s | 16.0 s, the warning |
| SST (the observation path, `_mountObs`) | 15.9 s -> 27.1 s | 15.5 s, the warning |
| wind (never clipped) | drawn at 1.3 s, no note; the coastline outlines join at 27.1 s | 1.0 s; no outlines (the coastline cancelled at 15 s) |
Every frame asked once (10 / 10 / 5 / 15 distinct requests, 0 repeated), particles on in every wave run.

### The patient first frame (`s/k2x.js`, desktop 1280x800 at 45 KB/s: the full-resolution 143-KB frame)
The first frame (`hs/f010`, 142,736 B) took 6.4 s (16.9 -> 23.3 s) beside the coastline and was kept, asked once: the old
5-s total limit would have cut it. Production asks it only after its coastline is cancelled (30.8 -> 34.4 s, alone on the link).

### Frame downloads after the first picture on a slow link (`s/k2x.js`, every frame kind counted; both sites watched 30-60 s after the picture)
| run | test 2.22.3 | production 2.21.7 |
|---|---|---|
| phone, 30 KB/s | the shown step's direction (`pdir` +10 h) and the next frame (`hs` +11 h) both cut at 5 s three times (36-51 s), then marked unavailable; nothing more asked until the coastline landed at 76.4 s; the panel's re-render then shows "Unavailable frames are skipped: +11 h"; no particles | 17 requests for 10 frames, 9 cut: `pdir` +10 h three times (unavailable: no particles for the shown step either), +12 h three times (unavailable), +11 h and +9 h cut before they got through; the field frames all landed |
| desktop, 45 KB/s | the first frame (143 KB) kept by the patient rule (6.4 s); then `pdir` +10 h and the full-resolution `hs` +11 h cut three times each, both unavailable, "+11 h" in the panel at 59.2 s; ends without particles | 15 requests for 9 frames, 7 cut: `hs` +11 h three times (unavailable, no note: no re-render), +12 h, +9 h, +8 h cut once or twice; the direction frames landed: particles on |
| desktop, 30 KB/s | as at 45 KB/s (the note at 83.5 s, no particles) | (not run) |
| the coastline alone slow (50 KB/s), the rest fast | 0 cut, 10 frames once each | 0 cut |
Section 41's frame watchdog limits each download to 5 s in all (`Math.max(STALL_MS, 4 x interval)`, three tries, then a
60-s cooldown); only the first frame of a mount has the no-byte rule (fix round 2). On these links an 84-KB direction frame
or a 143-KB full-resolution frame sharing the line needs more than 5 s, so production already throws such downloads away
(the frames were still arriving: each cut costs up to 5 s of the link). Fix round 2 adds the coastline to the shared line
after the first picture, so on the test site the next field frame and the shown step's direction frame are cut where
production's got through (desktop 45 KB/s: the test site ends without particles, production with them). Nothing asks a
failed shown step's direction again when its cooldown ends (pre-existing): the particles return only when the visitor
plays, steps or seeks. Reproduced in Node on a scratch copy of the playback suite (`node/repo/tests/overlay/ring_cut.test.js`,
last test: the coastline loading after the unclipped first picture, two ring frames still receiving bytes): on 2.22.3 both
are asked 3 times and marked unavailable, `transientFails` 2 (one more field failure and a seek or step shows the Retry
state). Two candidates tried on that copy: (a) the no-byte rule for every frame download (`patient = true`): the reproduction
passes, 169 of 170 playback tests pass, the one failing being the pin of the old rule ("a later frame keeps the total limit");
(b) the no-byte rule only while the coastline is loading: the reproduction passes, 170 of 170. Every hung-download test
(no byte at all) passes under both.

### R-2: each kind loads and draws on its own (`s/r2_live.js`, desktop 1280x800, the overlay saved Off, cache off; a slow file by a DevTools per-URL rule at 3,000 B/s)
| case | test 2.22.3 |
|---|---|
| `world-lines.bin` slowed (342 KB, ~115 s), both rows on, Europe z5 | the names (55) and the Places credit at 2.7 s; the lines (19 tiles) and the Borders credit when the file landed, 117.3 s (2.22.2, V9-1: the names waited for the lines file) |
| `world-places.bin` slowed (145 KB, ~48 s), both rows on, Europe z5 | the lines (19 tiles) and the Borders credit at 2.8 s; the names at 51.1 s |
| Borders off, City names on | `world-lines.bin` never asked; the names at 3.2 s; only the Places credit |
| City names off, Borders on | `world-places.bin` never asked; only the Borders credit |
| Borders off, `world-places.bin` slowed, Borders clicked on (trusted) at 6.4 s, Europe z5 | `world-lines.bin` asked at 51.0 s, when the places file landed; the lines drawn and credited at 51.5 s (V2-2) |
At the default Hawaii view the same runs give the same request order and credits (no border line falls in that view).

### Load order on a normal link (`s/v9_order.js`, fresh visits, cache on)
| case | load | coastline | first frame | settled() | places index |
|---|---|---|---|---|---|
| desktop 1280x800 | 423 ms | 568-572 | 766-771 | 887 ('ready') | 1,923 (load + 1,500) |
| phone 375x812 | 266 | 394-398 | 575-595 | 643 ('ready') | 1,769 (load + 1,503) |
| overlay saved Off | 270 | - | - | - | 1,778 (load + 1,508, nothing to wait for) |
The first frame is asked at load + 343 ms on the desktop and load + 309 ms on the phone (2.22.2, the same script: load +
691 and + 454 ms): it no longer waits for the coastline. On a normal link the places still start at load + 1.5 s, after the
overlay has settled. Names and credits as before; console clean.

### Regression (the G34 fix-round verification's scripts, same views)
- Names layout (`s/02_views.js`, storage cleared): 35 views at 1280x800 and 35 on the phone: 1,050 and 312 names, 0
  overlapping pairs, 0 cut, 0 covered, 0 partial; the name count of every view equal to the G34 fix-round run's.
- The names come back (`s/v9_return.js`): Europe z5 53 -> gear 50 -> 53 (the same set), tools menu 52 -> 53, San Francisco
  z10 66 -> the exposure fan 43 -> 66, New York z9 107 -> a live window 57 -> 107, the forecast window 45 -> 107; 0 overlaps
  / cut / covered in every state. Windows and panels (`s/05_windows.js`): 0 names under any of them (start 109, gear 104,
  tools 106, live 53, tide 64, wind 52, forecast 43). Tools (`s/08_tools.js`): 0 names over the fan, its labels, the
  rings' labels, a measure label or a forecast point.
- The grey no-reading ring (`s/v9_ring.js`): as before (transparent 2-px grey ring; production's white dot); a trusted
  click (desktop) and a tap (phone) open their wind windows.
- Failure paths (`s/10_fail.js`): every case as in the G34 fix-round run (refused, held index, the partial B-5 case, a cell
  500 then healed by the timer, a damaged cell final, the lake-outline set refused then attached, offline then healed),
  with the same counts.
- Model tiles = production (`s/b6_identity.js`, run 2026102318, valid 2026-10-25 10Z, lake run 2026102401): wave height,
  period and SST at the Great Lakes z6 and Okeechobee z8, 144 tiles, 0 different; wind differs only on lake edges (51,086 +
  2,630 px, 0 off a lake edge, 21 + 4 masked tiles).

### Timed playback (`w54p.js`, PREGC, frames warm at the edge for both Origins (1,463 HIT each), GPU, ABBA per cell, 20-s idles, 1920x1080)
| cell | production 2.21.7 | test 2.22.3 |
|---|---|---|
| Europe z5 wave height shark | 7.2 fps (gap p50/p90 125/175 ms, max 831: the run's lake frames cold at the edge, 238 MISS); 8.0 (125/133) | 7.8 (125/134; 242 MISS); 7.9 (125/137) |
| Great Lakes z7 wind shark | 7.9 (125/134); 7.9 (125/135) | 8.0 (126/135); 7.9 (125/135) |
0 skipped and 0 unpainted landings in every run; frames fetched once each (1.02 per landing; Europe's lake steps drew their
own lake frames, 0 bare / wrong); in flight never over the cap. The cold lake frames (the Europe view's one-screen margin
reaches the Great Lakes; the warm-up script covers the GFS frames only) hit the first run of each site alike. Memory (page
heap + array buffers vs Off): Europe live +39.2 / +39.2 MB vs production +39.2 / +39.2, peaks +64.0 / +61.3 vs +59.3 /
+61.0; Great Lakes wind live +36.5 / +36.4 vs +33.2 / +33.2 (+3.3 MB: the lake outlines' masks, as in the G34 fix-round
verification), peaks +62.0 / +75.4 vs +59.9 / +60.8: the +75.4 is one sample (the next highest +66.5; medians +46.7 / +48.0
vs +44.0 / +42.4), 4.6 MB over production's range + 10; repeated below. Main thread: Europe 819 / 804 ms/s vs 750 / 787;
wind 657 / 633 vs 618 / 630.

### Timed playback, the Great Lakes wind cell repeated (ABBA, the same run, warm)
production 7.8 / 8.0 fps (gap p90 139 / 135 ms), test 8.0 / 8.0 (136 / 133); 0 skipped, 0 unpainted; live +33.2 / +34.1 vs
+36.5 / +36.6 MB; peaks production +73.3 / +59.3, test +62.1 / +63.0. Over the four runs of each site, production's peaks
range +59.3 to +73.3: every test peak (+62.0 to +75.4) lies within that range + 10, and the live sets within production +
3.4 MB (the lake outlines' masks).

### Console smoke (a fresh page target)
A new page target, the storage empty, 1280x800: the first visit (UI 1.25.3, wave height restored, 2 names), Europe z5, Biscay
z9, Oahu z11, the world at z3, wind, the Great Lakes z6 under wind, SST, Off, wave height, the gear's Borders and City names
off and on (trusted clicks), the forecast window expanded and minimised, the swell-exposure tool and its x, a reload (66
names at the saved view): 0 console errors, warnings or exceptions. Afterwards the harness browser was stopped, its profile
removed, no process left; the test and production origins' storage emptied; the in-app browser pane was not used.

### Findings
Fix round 2 does what the owner chose: the first picture on a slow link now comes as soon as production's (it was 7-46 s
later, and never while the coastline trickled), it is clipped when the coastline lands, the note says what is happening,
and each location-aid kind draws as its own file lands. Two findings:
- **V2-1 (P2): on a slow link, frame downloads that are still arriving are thrown away after 5 s.** Section 41's watchdog
  limits each frame download to 5 s in all; only the first frame of a mount has the no-byte rule. Production already loses
  frames this way at 45 KB/s on a desktop (full-resolution frames) and at 30 KB/s on a phone (the 84-KB direction
  frames: the shown step's particles); with the coastline now loading beside the ring after the first picture, the test
  site also loses the next field frame and, on the desktop at 45 KB/s, the shown step's direction frame (no particles, where
  production has them), and the panel's re-render at the clip shows "Unavailable frames are skipped: +11 h". A failed shown
  step's direction is not asked again when its cooldown ends (pre-existing): the particles return only on play, step or
  seek. Candidates (scratch, above): (a) the no-byte rule for every frame download, which also ends production's cuts; (b)
  only while the coastline loads, which ends the new cuts only.
- **V2-2 (P3): a kind turned on during a slow load waits for that load.** `PlacesStore.load` returns the load in progress, so
  a row turned on meanwhile starts its file only when the other file has landed (45 s at 3,000 B/s); the re-sync then
  fetches it at once. A second load beside the first (its own controller and stall timer) would start it immediately.
Owner awareness (no change proposed):
- After the unclipped first picture the ring's field and direction frames share the slow link with the coastline, so the
  clip lands later than 2.22.2's clipped picture at 60 and 45 KB/s (46 vs 34 s, 69.5 vs 49 s) and earlier at 30 KB/s (76.6
  vs 83 s, where 2.22.2's 30-s places cap also took the link). Holding the ring while paused until the coastline lands would
  bring the clip back to about 2.22.2's time, at the cost of a ring that is not ready when play is pressed during that time.
- When the frame is fast but the coastline slow (the coastline alone at 50 KB/s), the frame is in hand after ~1.2 s and the
  picture still waits the full 15 s, as decided.
