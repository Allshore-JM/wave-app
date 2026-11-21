# G74a: adversarial review of plan section 106's frames-job changes (jet stream fields jet / jdir / jhgt)

Date: 2026-11-21. Scope: `feat/jet` @ 5dd8fe9 on top of production 8ad27ff: `git diff 8ad27ff..5dd8fe9`
(`tools/model_frames/{encode,decode,run,ecmwf,ecmwf_fetch}.py`, `tests/model_frames/{test_job,test_ecmwf}.py`, plus the
TEMPORARY `jet_dryrun_check.py` + `.github/workflows/jet-dryrun.yml`). Both frame jobs publish three new fields per step for
GFS and ECMWF: `jet` (250 hPa speed, full + half), `jdir` (direction FROM, half), `jhgt` (250 hPa height, half).

Two fresh-context reviewers, brief in the session's scratch (`jet106/g74a_brief.md`: six hard requirements):
- **A** (Fable): the code. 28 constructed index shapes against the new ECMWF `select()`, live GFS and ECMWF indexes, an
  independent decode of real 250 hPa records, 39 archived GFS analyses (both winters) for the encode ranges, 8 mutations.
- **B** (Opus 5.5): production impact and real data. Real manifests built with the branch, expanded to 209 / 85 frames and
  run through production's `overlay.js` (validateManifest, validateGrid, decodePngGrey, the 195-test playback suite with
  the jet fields added); jet / jdir / jhgt vs eccodes at every node on runs and steps the author had not used; requests,
  bytes and build time per ECMWF step; costs; 60 archived GFS dates.

**Result: 0 P0, 1 P1, 3 P2, 9 P3** (distinct; A 0 / 1 / 2 / 6, B 0 / 0 / 1 / 4; A-1 = B-1).

## What held up
- Every older field (hs, tp, wind, pdir, wdir, pres) of both models: the same records, CODE-identical frames to production's
  published frames of the same runs (author: GFS atmos + all ECMWF at 5 steps; B: 7 more steps, ECMWF objects also
  byte-identical); the ECMWF surface selection returns the old spans and every old guard still refuses (A, 28 shapes).
- Live browsers: production's client accepts the new manifests (B: no loop over `m.fields` in schema 3, no byte cap on the
  manifest fetch; +830 B, GFS manifest 14.1 KB raw / 1.9 KB brotli); the playback suite passes with the jet fields present.
- Values: jet / jdir / jhgt within half a code of eccodes at every node (B), conventions right (A: both models put the jet
  max near 41-42 N 155-160 E at ~110 m/s, mean direction FROM 274-275 deg at 30-50 N, GFS vs ECMWF height r = 0.99996).
- ECMWF requests per step 8 -> 11, oper bytes per run 168 -> 300 MB, fetch time +25-44 %, estimated build ~1,100-1,150 s
  against the 2,700-s budget; no new route from a throttle to a failed run (every new raise is an existing class).
- Costs (B): GFS 2,090 -> 2,926 objects a run (+117.8 MB), ECMWF 850 -> 1,190 (+41.6 MB); ~0.64 GB more kept; ~122k
  writes a month (~$0.55 at list price past the free tier).
- The temporary workflow uploads nothing and needs no secrets.

## Findings and what was done

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| A-1 / B-1 | P1 | `jhgt` floor 9,000 m: archived GFS analyses reach 8,901 m (2026-09-12), 8,970 m, 8,991 m in austral winter; the vortex core would clamp (contours stop, readout "<= 9000"). Top seen 11,173 m. | FIXED: 8,750-11,290 m, exactly 10 m a code (before any run is published, so retention never holds two ranges); comment + pinned tests. |
| A-2 | P2 | The rebuild guard's "a published jet range cannot change" was not pinned (a mutation skipping the jet fields passed). | FIXED: the guard test changes each jet field's range and expects a refusal. |
| A-3 | P2 | The six new identity rows were not pinned (4 mutations survived); no real 250 hPa record in the fixtures. | FIXED: rows asserted literally; new fixture `tests/fixtures/ecmwf/{meta,oper,wave}_24` (2026112100 step 24, data.ecmwf.int: the AWS mirror refused the older run for 40 min) with a test on the real records' identity, units (gpm), span and geometry. |
| A-6 | P3 | `select()`: an unhashable param / levtype now raised TypeError; non-pl u reported as "sfc"; comment said 13 levels (14). | FIXED: skipped / refused as RuntimeError; message names the expected levtype; comment 14. Tested. |
| B-6 | P3 | The dry-run workflow had no concurrency group. | FIXED: group `jet-dryrun`, cancel-in-progress. |
| A-4 | P3 | ECMWF budget not measured on the runner. | The dry run records both builds' elapsed time; read before STOP 1. |
| B-2 | P3 | Speed top 130 m/s leaves 7 m/s over the strongest sampled core (122.7 m/s, 2024-12-09). | Accepted; watch the run summary's "jet stream 250 hPa" clamped count this winter. |
| A-5 / B-3 | P3 | A missing / duplicated 250 hPa record now fails the whole build (waves, wind and isobars stop too). | Accepted, the same design as `pres` (G32a); all 209 GFS and 85 ECMWF indexes swept clean. Owner awareness. |
| B-4 | P3 | ECMWF READ_WORKERS 5 < 6 oper records (one read waits); GFS reads the three jet records as separate ranges (atmos 601 -> 1,000 MB a run). | Accepted: more parallel reads would throttle sooner; GFS bytes are free. |
| A-8 | P3 | The dry-run check's "truth" reuses the job's fetch/decode, so it proves the encode round trip only. | Accepted: record choice and convention are covered by A's and B's independent checks. |
| A-7, A-9 | P3 | No weakening of `select()`; other consumers of `encode.FIELDS` (Great Lakes, RRFS, alt) unaffected. | Notes. |

## Runner dry run (temporary jet-dryrun.yml @ 5dd8fe9, run of 2026-11-21 18:20-18:54 UTC, pinned eccodes 2.48)
- Jet check (GFS 2026112112 f000/f024/f120/f123/f240/f384, ECMWF 2026112100 f000/f024/f144): every jet / jdir / jhgt node
  within half a code of eccodes; every older field (GFS waves included: JPEG2000 on the runner) CODE-identical to
  production's frames, 10/10 at every step. PNG KB a GFS step: jet 300, half/jet 99, half/jdir 113, half/jhgt 38.
- Full GFS build 2026112112, no upload: 19:29 wall, 505 MB peak. Production built the same run in 827 s (13.8 min, with
  uploads): the jet fields add ~6 min (+41 %) a GFS build (three more ranged GRIB reads a step), inside the 120-min limit;
  each GFS publish lands ~6 min later. Owner awareness.
- Full ECMWF build 2026112100, no upload: 11:24 wall, 522 MB peak, 0 throttle lines (production built it in 856 s): inside
  the 45-min budget (A-4 closed).
- Run summary lines: GFS peak 220.3 kt, height 9,326-11,086 m; ECMWF peak 213.5 kt, 9,324-11,097 m; 0 clamped cells (old
  9,000-11,295 range; the fix round's dry run re-checks 8,750-11,290).

Suites after the fixes: tests/model_frames 976 passed, 1 skipped (one unidentified failure in one of three local runs while
two reviewers and other sessions loaded the machine; two clean reruns, CI green); the full pytest 3,122 passed before the fix round (scipy-only
bathy files excluded locally).
