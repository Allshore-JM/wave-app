# Overlays review gate G1 — frame publishing job (2026-09-22)

Scope: `tools/model_frames/*`, `tests/model_frames/*`, `.github/workflows/model-frames*.yml` on
`feat/overlays-1` @ 7be9ad3. Three fresh-context reviewers (data correctness; atomicity/ops/security;
NOAA source semantics verified against the live bucket). Every P0/P1 fixed in the follow-up commit on
the same branch; dispositions below.

## Verified true (no change)
Bucket key layout for all 81 steps of both products across three cycles; `.idx` keys unique for
HTSGW/PERPW (wave) and UGRD/VGRD `10 m above ground` (atmos), including f000 (`anl`); both products
on the identical 1440x721 0.25 deg grid, La1 +90, Lo1 0, scan 0x00 (read from GRIB section 3 of the
live files); `np.roll(-720)` gives col 0 = -180 E exactly; units and NCEP's PERPW definition (peak,
1/fp, despite the table name "mean"); crash atomicity of the pointer; the concurrency group; secrets
confined to the publish step; no fork/PR trigger; quantiser boundary behaviour; PNG chunk hygiene.

## Findings and dispositions
| Sev | Finding | Fix (commit on feat/overlays-1) |
|---|---|---|
| P0 | `--steps` subset flipped `latest.json` to a `complete:false` manifest and the equality short-circuit then froze it for the cycle; the test pinned the wrong behaviour | Pointer flips ONLY for a complete run; partial builds write `partial-<ts>.json`, never the pointer; a subset needs `--dry-run` or `--allow-partial` (the workflow forces dry-run for subsets); short-circuit checks `latest.complete`; test inverted |
| P1 | Pointer could regress to an older run: any transport error in `exists()` read as "absent", and `main()` only checked equality | `_request` retries 5xx/transport and raises `TransportError` (never "absent"); only 403/404 mean absent; `main()` refuses to publish a run older than the live one unless `--force` |
| P1 | Cron at cycle+4:30 preceded completion of every measured cycle (+4:36..+4:52 over 8 cycles) → overlay one cycle stale or skipped by chance | Cron `7,37 * * * *`; the current-run short circuit costs ~1 min of free runner |
| P1 | "f240 exists" is not completeness: NOAA back-fills 0-10 steps per product 13-16 min after their neighbours (12Z had a 4-min window where atmos f201 was missing) → mid-run 404 crash, orphan prefix | Completeness = every needed `.grib2` AND `.idx` (162 each) present in one ListObjectsV2 per product; a mid-run 404 → `NotReady` → exit 3 (retry next tick) |
| P1 | Half-res frames offset a quarter cell (PIL NEAREST picks odd columns, even rows); half grid absent from the manifest | Exact subsample `q[::2, ::2]` (361x720, same origin, 0.5 deg); `grid_half` in the manifest |
| P1 | Tp below 4 s (5.5 % of ocean points, min 1.09 s) silently decoded as 4.0 s | Encoding ranges widened (Hs 0-15 m, Tp 1-30 s, wind 0-80 kt) while the LEGEND ranges the owner chose are carried separately; `clamped_low/high` counts per frame; encoding_spec says q=1 means "<= lo", q=255 "\>= hi" |
| P1 | Fetched records never identity-checked | `decode(blob, key, run_dt, step)` asserts shortName/level/stepRange/cycle; ranged GET must be 206 with the promised length and one GRIB message; duplicate `.idx` keys raise |
| P1 | Workflow discarded Python's stderr (`2> time.txt`) | `/usr/bin/time -o`; `rc` captured without depending on the shell's `-e` |
| P2 | Immutable keys rewritable (`--force`, branch dispatch, encoding change) | Version segment in the prefix (`gfswave/0p25/v1`); manifest key carries the publish timestamp; `latest.json` names it |
| P2 | Manifest could contain `Infinity` | `allow_nan=False` + finiteness assert on decode |
| P2 | Manifest ambiguities (pixel registration, dlon/dlat, frame interval, periodic longitude) | Added `registration`, `dlon/dlat`, `frame_hours`, `expected_frames`, `lon_periodic`, `encoding_spec` |
| P2 | Schedules auto-disable after 60 idle days; keepalive missing | `model-frames-keepalive.yml` re-enables twice monthly; real monitor is `latest.json` age |
| P2 | No CI for the job tests | `model-frames-tests.yml` |
| P2 | Failure loop on a corrupt cycle | `failed/<RUN>.json` attempt log; retry after 3 h, max 3 attempts |
| P2 | `push` trigger publishes branch code into the shared bucket | Kept for Phase 1 only; removed at the job-only merge |
| P3 | prune counted orphan prefixes, ignored DeleteObjects errors, `keep<=0` | Keeps newest N COMPLETE runs, deletes older orphans, raises on errors, validates keep |
| P3 | `get_json` swallowed auth/network errors | Only "missing" is swallowed |
| P3 | Float32 quantiser exceeded the half-quantum bound by 5e-7 | Quantise in float64 |
| P3 | Wind constant 30.868 vs 60 kt; 60 kt = 69.05 mph not 70 | `60 * 1852 / 3600`; client legend label to use 69 mph |
| P3 | Unpinned action tags | Pinned to commit SHAs |
| P3 | Dispatch `steps` input could smuggle flags | Regex-validated; args passed as an array |

## Left open (tracked)
- Compare-and-swap on the pointer (`If-Match`) is not implemented; the concurrency group plus the
  ordering guard cover CI runs; local runs must not be executed against the production bucket.
- conda-forge package versions are unpinned (micromamba cache keyed on the spec).
- The client must treat q=0 as an absent neighbour in bilinear sampling and label clamped codes.
