# G31a — adversarial review of the SST tiles job (plan section 44, step 1)

Target: `feat/sst-overlay` @ 14190ba (86008f8 the job + the TEMPORARY `sst-dryrun.yml`), diff against production 66855bf.
One fresh-context reviewer (Opus 5.5, HIGH), no R2 access; ERDDAP and NDBC read-only with the site's verification
User-Agent. Inputs: the real MUR file of 2026-10-14 (754 MB), the author's local build of that day, the runner dry run.

## Summary

**0 P0, 0 P1, 3 P2, 7 P3, 1 nit.** The data are right: every value checked equals MUR within half a step, the geometry
is the documented one everywhere tested (date line, poles, aprons, the client's 20-degree cell names), the pointer is
written last, and prune stays inside `sst/v1/<8 digits>/`. The findings are about operations (the download, rebuilds,
time limits, alarms) and test coverage.

## Findings and outcomes

| # | P | Finding | Outcome |
|---|---|---|---|
| F1 | P2 | The "resumable" download can never resume: ERDDAP answers every `Range` request on `/files/` with **HTTP 416** (re-checked by the author with curl: `-r 0-99` and `-r 100-` both 416). A cut transfer failed every retry. | FIXED: one whole request per try, from byte 0 (a day takes ~10 s on the runner); no `Range` header is ever sent; the size must equal the listing's. Test fake answers 416 to any Range. |
| F2 | P2 | `--force` (or ERDDAP's revision of a day) rebuilt a day into the SAME "immutable" tile keys while the new manifest carried new per-cell lo / hi: a browser or the edge holding the old PNG (cached a year) would decode it with the new range: wrong temperatures. | FIXED: each build writes under its own directory `sst/v1/<day>/<BUILD>/` (BUILD = the build's start, `YYYYMMDDTHHMMSSZ`); the manifest's tier `dir` and `tier0.key` name it, and `build` is in the manifest. Prune still removes whole day directories. Test: two builds of one day share no key. |
| F3 | P2 | No time limit below the workflow's 60 minutes: a trickling mirror would be killed by GitHub (no failure record, no back-off; every hourly run tries again). | FIXED: `DOWNLOAD_DEADLINE_S` 40 min; past it the download raises `DeadlineExceeded` (not retried), recorded as a failure with its back-off. Test with a fake clock. |
| F4 | P3 | A listing outage failed every hourly run (an email each hour). | FIXED: a listing failure (or no MUR day in the listing) is quiet (exit 0) unless the LIVE day itself has gone stale, then the stale alarm (at most once in 12 h). A dry run still reports it (exit 1). |
| F5 | P3 | `--day` older than the live day raised a FALSE stale alarm (and silenced a real one for 12 h). | FIXED: the stale alarm runs only on scheduled runs (no `--day`) and always judges the LIVE day (the pointer), never the listed one. |
| F6 | P3 | After a day's 6th failure the job went silent (the skip path never ran the stale check). | FIXED: every quiet path (nothing newer, the listing fails, a day keeps failing) ends in the stale check of the live day. Nothing published at all counts as stale. |
| F7 | P3 | MUR's ice flag (mask 9) also covers some warm coastal water (14,835 pixels > 2 C, max 8.3 C, e.g. 68-69 N 49 E): small nearshore holes on Arctic coasts. | ACCEPTED (recorded): MUR's own flag; tiny globally, nothing near Hawaii or the US coasts. Revisit only if a user reports an Arctic hole. |
| F8 | P3 | `del mask` in `build` freed nothing: `main` still held the arrays (~650 MB through the whole build). | FIXED: `build(day, [sst, mask], ...)` takes the arrays over (the list is emptied; `main` passes a temporary list), so the mask goes once the value grid is made. |
| F9 | P3 | Stats: `ice` counted flag 16 and mask 13 counted as both lake and ice. | FIXED: lake = any pixel with flag 4 (5 and 13), sea ice = flag 8 without flag 4 (9). Test pins both counts. |
| F10 | P3 | 22 of 43 of the reviewer's mutants survived: coastal t1 / t0 node means counting land as a value, `node_fill` direction and pole rows, `NODE_FILL`, the drain before the manifest, the live `>=` check (pointer regression), `--local` uploading, read_mur's axes / shape / fill checks, constants. | FIXED for every survivor that could publish wrong data or break a stated promise (tests below); constants pinned. |
| nit | — | `failed/<day>.json` records were never pruned. | FIXED: prune removes failure records of days older than every kept day. |

Also removed before the merge: the TEMPORARY `.github/workflows/sst-dryrun.yml` (a test asserts it is gone).

## Tests added or changed (`tests/model_frames/test_sst.py`)

- Download: restarts from byte 0 against a 416-on-Range fake (no `Range` header, ever); gives up after `tries`; a short
  or long body refused; the deadline with a fake clock (one try, `DeadlineExceeded`).
- Builds: the build directory in every key and in the manifest; two builds of one day share no key; every tile stored
  before `build` returns with a slow, threaded bucket (the drain before the manifest); the arrays taken over.
- Main: an older listed day never replaces the live one (the `>=` check); a listing outage quiet while fresh, the alarm
  once when stale, a dry run reports it; `--day` never alarms; a day that keeps failing ends in the alarm.
- Prune: build directories go with their day; failure records older than every kept day go, newer ones stay.
- Tiles: coastal node means = the mean of the valued pixels only (every land-touching node of the island, against an
  independent block mean); `node_fill` takes each of the 8 neighbours from its own side (parametrised) and never wraps
  across the poles; the stats' lake / ice counts.

## Mutation

40 mutants of `sst.py` / `sst_tiles.py` (the step-1 set without the two download-resume mutants, which no longer apply,
plus 17 aimed at the fixes and the reviewer's survivors): 38 killed on the first run; the two survivors were real test
gaps (no mask-13 pixel in the test world; the pole test checked one direction only), pinned by two new tests, then
40 of 40 killed. Further pins: `read_mur` refuses shifted axes, another fill value and another shape; `--local` never
touches the bucket; a fill needs one valued neighbour; the job's constants (COAST_PX 3, FILL_PASSES 2, NODE_FILL 1,
STALE_DAYS 4, alarm every 12 h, 6 attempts, 1-h back-off). `tests/model_frames/test_sst.py`: 45 tests.

## What held (from the reviewer's report)

- Geometry: the documented formula = MUR's pixel centres (t2) and the 5x5 / 25x25 block centres (t1 / t0) at every point
  tested, incl. the date line (lon 180.00 is MUR's last column), cell corners, aprons and high latitudes; t0 = 1440 x 721,
  lon0 -180, lat0 90 = the frames' grid; 20-degree names -100..80 = the client's `coastCellsForTile`.
- Values: exhaustive independent match (22 cells, 0 NaN-pattern differences, error within half a step); MUR's coastal land
  values agree with the sea within 3 px (881,649 used: median |diff| 0.0125 C); lakes and ice empty; Black Sea, Baltic,
  Red Sea, Persian Gulf, Pearl Harbor, Kaneohe, Honolulu Harbor, SF Bay, Chesapeake, Puget Sound ... every sea pixel
  valued; t0 never clamped on 2026-10-14 (max 33.64 C).
- Publishing: 1,990 PNGs written = listed, byte counts equal; stats, manifest, pointer order; deterministic builds.
- Workflow: actions pinned by SHA, env pinned, inputs through env (no injection), its own concurrency group, keepalive.

## Could not do (reviewer)

Runner-side measurements (done by the author: GitHub run 38854801153, 1:31 wall, RSS 3.3 GB); R2 (no secrets); a
GSHHG-water vs tile-coverage count (the archive was outside the allowed network list).
