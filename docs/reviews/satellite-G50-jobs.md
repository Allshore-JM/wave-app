# G50: the satellite jobs (plan section 81, step 2) — adversarial review record

Reviewer: one fresh agent (Opus), 2026-11-04, on `feat/satellite` @ efb6cd0: `altfmt.py`, `alt.py`, `sarswell.py`,
`test_alt.py`, the workflows and pins. It ported `overlay.js` `sampleRow` to Python and compared it with
`sample_bilinear` on 200,000 random points (0 mismatches); probed the prune race, the back-off, the chunk format and
the swell reader's wordings with scratch scripts. Network blocked: real files not checked.

| # | Sev | Finding | Fix (commit after efb6cd0) |
|---|---|---|---|
| 1 | P0 | `sarswell.py`: the direction check joined every attribute and took "clockwise **from** north" for FROM; "towards" / "(to)" / "propagates" passed | conventions from `standard_name` first (`*_from_direction` / `*_to_direction`, `*_at_variance_spectral_density_maximum`), else `long_name` with the reference phrase stripped; both words = refused; comment / description never read; tests with the three wordings |
| 2 | P1 | `alt.py prune`: a superseded build older than 2 h by its STAMP was deleted in the run that superseded it (clients on the previous pointer 404) | the pointer's `superseded: [{key, at}]` dates the supersession; prune waits BUILD_STALE_H from it; test |
| 3 | P1 | a 30-min frames-host outage (or download errors) charged 3 failures to each window's digest and dropped the windows for good | a frames outage is charged to no window (the run ends quietly); the back-off retries every FAILED_RETRY_AFTER_H (2 h) instead of giving up; test |
| 4 | P2 | the model column frozen at build time (a newer run never re-collocates) | the live run joins the digest of windows ending after its cycle |
| 5 | P2 | readings older than r0 - 18 h get no model value; undocumented | README "Known limits" |
| 6 | P2 | the mission-silence warning could never fire (computed from the 30-h listing) | from the pointer's `missions[].newest_utc` (per-chunk `newest_by_mission`) |
| 7 | P2 | `--window` failures exited 0 | 1 on every failure path of a manual run |
| 8 | P2 | every STAC item fetched every run (~3,000 GETs a day) | items filtered by the dataset pattern before the GET; STAC read every 6 h (`datasets_utc`), the pointer's list between |
| 9 | P2 | `decode_chunk` could inflate ~1 GB before the length check | `decompressobj().decompress(data, n x stride + 1)`, eof and tail checked |
| 10 | P2 | OGDR de-duplication point by point: CMEMS's rejected readings came back as OGDR dots | dropped by pass coverage (gaps <= 60 s, 1.5 s slack) |
| 11 | P2 | failed/ records of windows that never had a chunk never pruned | the failed/ prefix pruned by window age |
| 12 | P2 | a refused swell file only reached the summary as `STALE:` 12 h later | `WARNING: <file> unreadable: <reason>` in the summary and the failure record |
| 13 | P2 | a model range change failed every run (144 red runs a day) | alarmed once per 12 h through the stale-alarm record; nothing published |
| 14 | P2 | prune's whole-window branch ignored the pointer | windows the pointer names are never deleted whole |
| 15 | P2 | swell "newest production" keyed by (token, t0): unverified | noted in README; checked on the real listing (pending) |
| N | nit | RecursionError on a deep header; `run` / `flags` / `n` / `dir` / segment-mission unchecked; a partial chunk's header carried the digest; OGDR uncredited; 404 retried; `--no-model` kept the old model block; bytes attributes | all fixed (header `inputs` null on a partial chunk; the OGDR credit joins the chunk's credit; 404 at once; model block none without collocation; `_attr` decodes) |
