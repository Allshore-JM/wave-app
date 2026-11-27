# G80 adversarial review: FES2022 coast points (plan section 113 C)

Change: commit 815597e on feat/tides-113-c (parent 6565c2b, production). Reviewed 2026-11-27.

Tests: `python -m pytest -q tests/test_tides_coast.py tests/test_tides_model.py` passes (23).

Evidence: the probes are throwaway scripts in the session scratchpad (`g80/p1..p11`). They ran against the pinned
Natural Earth file (sha256 checked by `coastline()`), the committed snapshots and the local GeoNames `cities1000.zip`.
No FES data was used. A rebuild from the pinned file gives exactly the committed 16,892 points: same ids, same
positions.

## Findings, most severe first

### G80-1 (P1): a third of the coast points are named by position, not by a place (owner rule), and a third share a name

- Where: `tools/tides/fes_points.py:354-360` (`point_name`), `:340`.
- Scenario: with the GeoNames file the job uses (cities1000, populated places, 40 km), I ran `point_name` on the
  committed points:
  - 5,704 of 16,892 (34%) are named "Coast <lat>, <lon>". This covers most of Patagonia, northern Canada, Siberia,
    the Australian north, Kara-Bogaz-Gol and the Antarctic Peninsula. The owner's rule is that every point's name is
    a proper place name. A position is not one.
  - Of the points that get "Off <town>", 6,385 share their name with at least one other point (2,223 names). Examples:
    12 points are "Off Portree", 11 "Off Labutta", 10 "Off Pedernales" / "Off Paamiut". Neighbouring icons open
    windows with the same title.
- Fix:
  - Name the points in `build_coast_points.py` and store the name in the committed `coast_points.json`. The names
    can then be reviewed in the diff. Also, `fes_points.py` is in the constants' cache key, so today any naming fix
    forces the ~3 h FES re-download.
  - Use a fuller naming source: for example GeoNames allCountries features of class T (CAPE, PT, ISL) and H (BAY,
    COVE, INLT) within about 25 km, then admin1 plus country ("Coast of Aysen, Chile"). Leave out BCH-type features
    and anything that could be a surf spot.
  - To tell neighbours apart, add distance and bearing ("18 km SW of Portree").
  - Add a test that no coast name matches `^(Coast|Sea) -?\d`.

### G80-2 (P2): a failed set check throws away the 3-hour constants, repeats the download on every run, and stops the forecast points from being refreshed

- Where: `tools/tides/fes_points.py:307-317` (`os.remove(args.partial)` runs before `check_sets`);
  `.github/workflows/tides-fes.yml` "Encrypt the constants" (runs only after the Predict step succeeds).
- Scenario:
  - The first run after the merge misses the cache, because the key now includes `coast_points.json`. It downloads
    all 34 files.
  - In Predict, `finished(partial)` runs, then the partial file is deleted, and only then is `check_sets` called.
  - Suppose the coast set falls short of the new 50% threshold. This share has never been measured on real FES
    data; see G80-3 for the waters where points can die. Then SystemExit fires. The Encrypt step is skipped and
    nothing is cached.
  - Every later run (Monday and Thursday) repeats the ~3 h AVISO download and fails the same way.
  - The 452 forecast model points stop being republished, although their own 5% check passed. About 45 days later
    their files no longer cover the window (`tide_global.model_payload`: "does not cover the window"). Every model
    point then answers an error.
- Fix:
  - Write and encrypt the constants before the share check: the check should guard publication, not caching. At
    minimum, delete the partial file only after the check passes.
  - Check and publish each set on its own: drop a failing coast set with a loud log line and a failed job status,
    but still publish the forecast set.
  - Add a test that a failing check leaves the partial or constants file in place.

### G80-3 (P2): coast points in lagoons and enclosed waters take a tide from across land; the claim that lake and Caspian points "drop out" is not proven

- Where: `tools/tides/fes_points.py:174-188` (`sample`: the nearest native-ocean cell within 15 km in any direction);
  `tools/tides/build_coast_points.py:13-14` and the README (the claim).
- Scenario:
  - The walk puts points on every Natural Earth shore, including enclosed waters: 225 points in the Caspian box, 16
    in Kara-Bogaz-Gol, about 27 on the inner shore of Lagoa dos Patos, 19 around Lake Maracaibo, 7 in the Sivash, a
    point in the Bardawil lagoon (31.08 N 32.98 E), and points up the St Lawrence past Sorel (46.1 N 73.0 W).
  - `sample()` takes the nearest native-ocean cell within NEAREST_KM, whatever lies between. It does not check that
    the water is connected.
  - Take a lagoon behind a barrier 5-15 km wide that the AVISO mask does not mark as native ocean. Its points get the
    open sea's tide (for Patos, the Atlantic's) and are published as that place's tide.
  - If the mask does mark a basin as native ocean (whether the Caspian is ocean or "lake" in `mask_fes2022B.nc` is
    not checked anywhere), the points are kept, against the docstring.
  - No test or job log shows what happens to these points.
- Fix:
  - For `set == "coast"`, accept the bilinear value or a nearest cell within about 1-2 cells (5 km). Better: accept
    only a cell in the same connected native-ocean patch as the window cell nearest the point.
  - Exclude the Caspian, Kara-Bogaz-Gol and named lagoons with explicit boxes in `build_coast_points.py`.
  - Have the job print the kept counts per region box (Caspian, Black Sea, Baltic), so a run proves the claim.

### G80-4 (P2): no switch: the first job run from any branch puts about 17,000 points live on every site that reads the bucket

- Where: `tide_global.py:305-337` (unchanged in production: any valid `model:*` row is listed); `tools/tides/fes_points.py:376-401`;
  `.github/workflows/tides-fes.yml` (`upload` defaults to "true" on dispatch, one bucket and prefix `tides/fes/v1`).
- Scenario:
  - Production (6565c2b) already lists every row of `tides/fes/v1/points.json` as an "M" station.
  - Dispatching tides-fes on feat/tides-113-c to try it out publishes the coast points. Production lists them within
    the hour, before any prod-merge approval. There is no site setting to hide them.
  - Every site that reads the same bucket gets the same points. Whether the test site does could not be checked from
    here; the other publishers share the model bucket.
  - Rolling back means running the old code, and its upload deletes nothing, so the ~17,000 coast point files stay
    in R2.
- Fix:
  - Add a set marker to each index row, or publish a separate `points-coast.json`.
  - Have the site accept coast rows only with an env switch (for example `TIDE_MODEL_COAST=1`), set first on the
    test site.
  - Make the dispatch input default to "false" on branches other than the default.

### G80-5 (P3): deleting stale files right after the index goes up races the readers, ignores R2's errors, and can mass-delete silently

- Where: `tools/tides/fes_points.py:395-415`; `coast_points()` at `:96-99`.
- Scenario:
  - Who still has the old list:
    - The edge keeps `points.json` for up to an hour (max-age 3600).
    - The site reads the list at most hourly.
    - An open page never reads the list again.
  - For up to about 2 h, a removed id is still listed. Its file is gone, so the window errors and retries.
  - `delete_objects` runs in batches of 1,000 and never reads `Errors`, yet the log says "removed N". The repo's own
    `tools/model_frames/publish.py:21,113-119` uses 250 and raises on Errors, after R2 timed out on 1,000-key calls
    on 2026-11-03.
  - If `coast_points.json` is renamed or missing, `coast_points()` silently returns []. The coast check is then
    skipped (no points), and the run deletes every coast point file.
- Fix:
  - Read the previous `points.json` before the upload. Delete only keys that neither index lists, or defer the
    deletion to the next run.
  - Use batches of 250 and raise on `Errors`.
  - Refuse to delete more than, say, 10% of the keys.
  - Fail when `COAST_PATH` is missing.

### G80-6 (P3): the station list grows fourfold, and its ETag will change on every run

- Where: `app.py:1281-1289` (`_tide_list`), `tide_global.py:336-337`, `templates/index.html:3449-3528`.
- Measured: `/api/tides/stations` grows from 5,859 to 23,203 rows: 474 KB to 1,884 KB raw (122 KB to 452 KB gzip).
  The origin does no compression (no gzip in `app.py`).
  - The single worker (start.sh: `--workers 1`) holds about 12 MB more (tracemalloc). Refresh plus ETag takes about
    0.45 s, hourly, partly under `_TIDES_LOCK`.
  - The page parses 23k rows and sorts them in `tideRevealZooms`, once per list.
  - The busiest desktop view at zoom 9 has about 130 coast icons, so the per-pan cost is acceptable.
  - The job downloads GeoNames unpinned on each run (`--geonames` is not passed). Any GeoNames edit to a used name
    changes the index, then the ETag, and every visitor downloads the list again twice a week.
- Fix:
  - Store names in `coast_points.json` (as in G80-1) or pin the GeoNames file.
  - Consider serving the M rows by view box, or compressing the list at the origin.

### G80-7 (P3): coverage holes the owner's "every 25 km" rule does not expect

- Where: `tools/tides/build_coast_points.py:88-98` (`taken_points`), `fes_points.py:235` (the land test also applies to coast points).
- Measured:
  - Keyed-agency stations count as taken even when the agency is off. Without a CWA key, 44 Taiwan coast points are
    missing and the site shows no tide point there at all.
  - Forecast points block coast points within 15 km whether or not FES then keeps them: 61 coast points.
  - The land test (`coast`) also applies to points that lie on the coast by construction. The only point on each of
    336 islands with under 12 km of coastline (for example Sala y Gomez, Ducie, San Felix) survives only if FES's
    1/30-degree mask has a land cell for that island.
- Fix:
  - Leave out the keyed agencies' stations, or have the site hide a coast point near a station it actually serves.
  - Block coast points only with forecast points that end up with a model tide (filter in `all_points` after the
    constants).
  - Skip the `coast` requirement for `set == "coast"` (filter the Null island feature instead, see G80-9).

### G80-8 (P3): test gaps: what is claimed but not proven

- Where: `tests/test_tides_coast.py:70-79`, `:101-122`, `:125-138`.
- Gaps:
  - The 15 km clearance is checked on 400 of 16,892 points. The full check takes 0.1 s. A snapshot change without a
    rebuild passes about 97% of the time.
  - Better: filter coast points against the current stations in `coast_points()` at job time, and run the full check
    in the PR suite.
  - The vectorised-search test never covers the longitude wrap, the clipped edges of the grid, or exact ties. I
    checked it myself with 9,000 points on the real 1/30-degree grid (wrap at 0/360, bands near 66 N, 0 and 60 S,
    tie points on cell midlines): 0 mismatches with the old loop. So the claim holds; only the test is missing.
  - Nothing tests the lake/Caspian claim (G80-3), or what a failed check does to the cache (G80-2).

### G80-9 (P3): coastline hygiene

- Where: `tools/tides/build_coast_points.py:59-64, 71, 113-118`.
- Findings:
  - Natural Earth's "Null island" feature (featurecla "Null island") is walked. It produces
    `model:c0900018000` at (-0.0044, -0.0048), zone `Etc/GMT`, in the committed file. FES's land test drops it later.
    Filter on `featurecla == "Coastline"`.
  - First vertices are not longitude-normalised. A line starting at +180 is stored in grid cell 180, which
    `Grid.near` never looks in. This is latent today: the 6 such lines all lie poleward of 66 degrees. Normalise with
    `(x + 180) % 360 - 180` in `walk()`.
  - 166 ids differ from `point_id` of the stored, rounded position. The id comes from the unrounded position. This is
    harmless unless someone ever rebuilds ids from the file; say so in the docstring.

## Checked and found sound

- Ids: unique and valid (`ID_RE`). Stable across rebuilds (same output from the pinned file). +180 and -180 give one id;
  no negative zero. No forecast station id can collide (no station id is "c" + digits).
- `walk()`: the carry across segments, zero-length segments, and long segments (largest 55 km). The pinned file has
  no line that crosses the date line. Points at lon -180 are stored as -180.0.
- Zones: no point's zone is more than 3 h from its longitude's solar offset. Names such as Asia/Manila for Western
  Australia, Europe/Paris for Poland, or Asia/Sakhalin for New Caledonia come from timezonefinder 6.5.9's merged
  zones. Their offsets and DST rules from now on are the same, and the page shows them as GMT+N. Nine remote-island
  points get `Etc/` zones (Heard, Bouvet, Baker, Palmyra, Tromelin): acceptable.
- Job time and memory, measured on the laptop with a synthetic global 1/30-degree grid and 20,850 points:
  - `sample` takes 2.3 s a file (1.3 min for all 34).
  - LAT takes 0.7 min.
  - Predictions take 11.8 min (17.5 KB a file, about 365 MB in all).
  - With the ~3 h download this is well inside 330 min, and the arrays need under ~1.5 GB.
  - R2: about 20,850 Class A puts a run (about 180k a month), plus about 21 list calls.
- Licence: only predictions and the index are uploaded. The constants stay encrypted in the cache, and
  `coast_points.json` is public-domain geometry.

## Fix round 1 (2026-11-27)

| Finding | Fix | Test (tests/test_tides_coast.py) |
|---|---|---|
| G80-1 | Names made once in `build_coast_points.names()` and stored in coast_points.json: "Off <GeoNames town>" for the point nearest a town within 15 km, else "<km> km <direction> of <town>" (300 km), else the position; unique (16,952 names, 5,999 Off / 10,509 distance / 444 position). The cache key is `fes_points.py --cache-key` (positions + CONST_VERSION), so names and code that do not change constants keep the cache. | `test_g80_1_coast_names_...`, `test_g80_1_the_cache_key_...` |
| G80-2 | Constants cached unless every set is short; a short set is not published, its rows online carried over, the job fails after publishing the other set; the workflow encrypts and saves with `always()` (actions/cache restore + save). | `test_g80_2_one_set_short_...`, `test_g80_2_a_short_set_keeps_its_rows_online`, `test_g80_2_7_main_...` |
| G80-3 | A coast point needs ocean within 6 km of itself (COAST_NEAREST_KM); the m2 log counts them. | `test_g80_3_7_...` |
| G80-4 | Only Live-Buoy-Update uploads (the workflow's UPLOAD expression); the site's FES_COAST=0 hides coast rows (set column or id pattern). | `test_g80_4_the_site_switch_...` |
| G80-5 | Deletes only files neither the old nor the new index lists; 250 a request; Errors raise; a missing coast file stops the job. | `test_g80_5_...`, `test_all_points_and_the_checks_per_set` |
| G80-6 | Names stored in the file: the station list's rows stay the same between runs (ETag). Payload: ~23,000 rows, ~450 KB gzipped once per visitor with the Tide layer on, then 304s: accepted (the page's per-pan work was found fine). | (ETag stability by construction) |
| G80-7 | Coast points no longer blocked by forecast points in the build; the job drops one within 15 km of a forecast point that got a model tide; no land test for coast points. Keyed-agency stations still block coast points (Taiwan has CWA's 262 places on production: accepted). | `test_g80_7_...`, `test_g80_2_7_main_...` |
| G80-8 | The committed file's 15 km clearance checked for every point; the vectorised search vs the loop at the grid's lon wrap and southern edge. | `test_the_committed_coast_points`, `test_g80_8_...` |
| G80-9 | "Coastline" features only (no Null island); first vertices longitude-normalised; the id/rounding note in the docstring. | `test_g80_9_...` |

Mutants (scratchpad mut80.py): 15 of 15 caught.
