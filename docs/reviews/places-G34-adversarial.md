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
