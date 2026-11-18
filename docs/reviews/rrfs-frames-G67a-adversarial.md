# G67a: adversarial review of the RRFS overlay frames in the job (plan section 99 D1)

Reviewed: feat/rrfs c877e81 (`tools/model_frames/rrfs.py` frames, merged manifest, overlay pointer, prune; tests;
README; workflow comment). Fresh-context reviewer, read-only, both laptop dry runs (cycle 2026111723, run 2026111718)
checked against the cell files, 30 mutants on a scratch copy. 2026-11-18.

Result: 0 P0, 2 P1, 3 P2, 4 P3. All fixed or recorded below; re-verified the same day.

Checked and fine: every sampled frame node equals its cell file's value through `rrfs_grid.locate` (about 96k nodes,
0 mismatches: missing, calm, row 0 north, Alaska across 180); the main pointer with `frames` blocks stays far under the
site reader's 256 KB cap and `rrfs_wind.py` / `tools/rrfs_verify.py` ignore the new key; frame CPU is ~62 s a run
(downloads dominate); publish order and prune keep everything a live or previous pointer names.

| # | Finding | Outcome |
|---|---|---|
| P1-1 | The manifest's `run` (the newest source's cycle) repeats every 6 h (a run lands ~1 h after a newer hourly cycle and changes hours +19..+84) and goes back when the top source turns stale; a browser adopting by `run` or caching frames by run keeps old hours | FIXED: the manifest's identity is its `signature` (frame keys, valid times, steps, the regions' grids / coverage / ranges; never the publish time), in the manifest and the pointer. The client (D2) adopts by signature and keys its caches with it. Test: same `run`, different signature and manifest key |
| P1-2 | Grids and ranges came from code constants: after a grid change (the D0 perf gate may coarsen CONUS / Alaska), up to 18 h of manifests would pair old-grid PNGs with the new grid (blank or misplaced wind); a removed region left keys the client rejects | FIXED: each source records its regions' grids and the fields' ranges (frames.json and the pointer's `frames` block); the manifest uses a source's region only when its grid and ranges are today's, names only regions it lists, and signs the grids. Tests: an old-grid source keeps its other regions only; other ranges: none; a removed region never named |
| P2-3 | All sources stale: the last manifest stayed live for ever | FIXED: the overlay pointer is withdrawn (`complete: false`, no manifest; written once); prune then drops the old manifest. Test |
| P2-4 | The overlay sync ran only after a publish or on an empty tick: failed / not-ready / back-off ticks never synced | FIXED: `_frames_step` (sync + frame prune) runs at the end of every live tick (idempotent: one read when nothing changed). Test: a failing build still syncs |
| P2-5 | A sync error after the main pointer was live gave a failure record for a live source and skipped the prunes; a corrupt overlay pointer never healed | FIXED: the sync is wrapped (busy: exit 3; else a summary line and exit 1, never a failure record); an unparseable overlay pointer counts as none and is rewritten. Tests |
| P3-6 | A dropped source's frames.json (the only record of why) was pruned at once; one oversize frame dropped all four regions | FIXED: prune keeps the frames.json of every kept source; an oversize frame drops its region only (the workflow still fails loud with the reason). Tests |
| P3-7 | The region index was built lazily inside the encoder threads | FIXED: built on the main thread before the first encode |
| P3-8 | CONUS (0.05 deg) and Alaska (0.07 deg) masks sit on different lattices: 32 of 11,281 drawn Alaska nodes have a drawn nearest CONUS node | RECORDED: harmless; the client's region order (hi, pr, conus, ak) decides there, as the tables' `locate` does |
| P3-9 | Test gaps: the dropped path's exit 1 came from the stale alarm; frame-to-step mapping untested; the stale boundary untested | FIXED: the dropped test stubs the alarm and asserts the WARNING; a 20-step test with u = step m/s decodes each frame to its own step; boundary tests at exactly 8 h / 18 h |

Added with the fixes (found while writing the client): a region's lat/lon BOX is far larger than its data (Alaska's box
reaches 40.5 N across the whole West Coast), so the browser would fetch empty Alaska frames over California. Each
region in the manifest now carries `cover`, a 1-degree mask of the cells with drawn nodes (hex, 14-1,053 characters);
the client fetches a region only when the view is near a set cell. Test: the mask equals the drawn nodes' cells, and
Alaska has none off California.

Re-check: 13 fresh mutants on the fixed code (stale boundary, dropped exit, the encoder writing a neighbour's step,
regions from the top source only, grids / ranges unchecked, no withdraw, a signature without the keys, a corrupt
pointer raising, no end-of-tick sync, frames.json pruned, the cover off by a row, a per-region drop dropping all): all
killed. tests/model_frames/test_rrfs*.py + tests/test_rrfs_grid.py: 115 passed.
