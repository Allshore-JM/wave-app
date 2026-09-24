# Overlays — G5 adversarial review (coastline clip, client + coast data) — 2026-09-24

Gate G5 of plan section 19 ("Coastline boundaries"): the browser now clips the wave-height and
peak-period overlays to GSHHG coastlines (asset v2.6.1, `static_overlay/overlay.js`), using the
coast-v1 data built by `tools/coast/build_coast.py` and published under `static/coast/v1/` in the
frames bucket. Reviewed on `feat/overlays-coast` @ abd0791 (test site wave-app-clean.onrender.com,
asset 2.6.1) before the production merge. Three fresh-context reviewers at MAX effort, no access
to the author's reasoning:

- **A — client code** (Node harness, read-only): the coast-v1 decoder, tile geometry at world
  copies / the dateline / the poles, rasterisation and the nonzero union, `composeTile`, mask
  caching and invalidation, `readoutAt` = drawn pixel, lifecycle races (Off, field change, Update,
  chunk arrival, the 15 s watchdog), resource bounds (in-flight, LRU, masks), tests, security.
- **B — coast data and builder** (Python + the published bucket objects): GSHHG reading, ring
  normalisation (0..360 storage, Greenwich, the dateline, the polar ring), cell clipping and seams,
  quantisation, the coast-v1 encoding and `--check`, the published files decoded and spot-checked
  against known coordinates, the JS decoder vs the Python decoder, tier 0/1 consistency, workflow
  pins/secrets, bucket safety, LGPL compliance.
- **C — owner requirements on the test site** (the only reviewer with the browser): "extend to and
  stop exactly at all landmass coastlines" for the wave and period displays at zooms 5–11 around
  every Hawaiian island and in other regions, readout = drawn pixel, the tier switch, playback cost,
  section-7 regressions, phones, memory, console.

Severity: P0 = wrong picture/data reaching users, crash, hang, unbounded resource; P1 = owner
requirement not met, or a race/resource bound broken in realistic use; P2 = robustness,
performance, maintainability; P3 = nits. Every P0/P1 is fixed and re-reviewed before the merge.

**Outcome: 0 P0, 2 P1 (A1 client failure path, B P1-1 licence), 6 P2, 18 P3.** The coastline
itself was found correct by all three: C sampled ~1.9 M pixels in 26 views (Hawaii at zooms 5–11,
the dateline, a world copy, Antarctica, California, Tokyo, the Baltic, Bergen, a phone) with 0
readout/drawn mismatches and the clip edge on the imagery shoreline within a pixel; B found 0
land/water mismatches in 24,000 random points over 16 regions against an independent test on the
raw GSHHG rings, exact seams on the dateline and every cell line (bar 1-unit quantisation ledges),
and all 1,474 published objects byte-identical to a local build. Fixes: asset **2.6.2** (@ 2974ef9)
and **2.6.3** (@ 37cfeb4), builder commits 2974ef9 and c3ca20d; test count 368 pytest (18 coast) +
43 Node.

## Findings and fixes

| # | Sev | Finding (reviewer) | Fix |
|---|---|---|---|
| A1 | P1 | A 404/410/decode failure of an INDEXED tier-1 cell was treated as "no land": the field covered the whole 5° cell's land for the session, no warning, no recovery (only an absent cell may mean ocean, and absent cells are never requested). | `CoastStore.setsFor` hands back tier 0 as a complete stand-in for a permanently failed indexed cell (`failed[name] === true`), so the tile keeps the 1 km coastline; test "404 stand-in". (2.6.2) |
| B P1-1 | P1 | LGPL redistribution incomplete: no copy of the licence or of GSHHG's permission notice anywhere, README said "version 3 or any earlier version", no modification notice, the only credit a bare panel sentence. | `tools/coast/LICENSE-GSHHG.txt` (dated modification notice + the archive's LICENSE.TXT + COPYING.LESSERv3), README corrected ("version 3 or later", citation); the builder publishes it first as `static/coast/v1/LICENSE.txt` (a new object, nothing overwritten); the panel credit reads "coastlines from GSHHG (Wessel & Smith), LGPL" and links to that notice (`rel="noopener license"`, text nodes only). Publish: see "Licence publish" below. |
| A2 / C#6 | P2 | A transiently failed chunk (5xx, blip, 15 s stall) was never retried on a static view: the ~1 km tier-0 edge stayed on screen through playback until a pan/zoom. | The store arms one timer for the earliest cooldown expiry (`_armRetry`), which bumps `rev` and calls `onChange`; the layer re-asks `setsFor` for its incomplete tiles and the chunk is fetched. Cleared by `abortAll()`. Test with `retryMs = 40`. (2.6.2) |
| A3 | P2 | The same-pixel vertex decimation dropped the builder's clip points on cell lines, opening sub-pixel slivers of field colour along every 30°/5° line inside land (worst 37/255 measured). | Vertices on the piece's own bbox edges are exempt from decimation; test "keeps the clip points on a piece's bbox edges". (2.6.2) |
| A4 / C#2 | P2/P3 | Every chunk arrival re-rasterised and redrew every incomplete tile (5.6 redraws per pending tile, 246 stand-in rasterisations in C's session). | `_landFor` reuses a stand-in whose key and sets are unchanged; `_redrawIncomplete` redraws only tiles whose mask object changed; test "a stand-in that is still a stand-in is not re-rasterised or redrawn". (2.6.2) |
| B P2-1 | P2 | `coast-build.yml` swallowed the build/check/upload exit codes behind `tee`/`tail` pipes (a failed `--check` or a refused upload showed green). | `shell: bash` with `rc` capture on both steps, as in `model-frames.yml`. Verified live: run #6 went red on a check problem (see the fix round). |
| B P2-2 | P2 | Nothing enforced "never overwrite the immutable prefix": any re-dispatch with `upload=true` rewrote year-cached objects. | `upload()` reads the published index and refuses a different build (`same_build`: tier-0 sha256 + the cell map); the same build only refreshes `LICENSE.txt` and `index.json`; `--replace` / the `replace` input overrides deliberately; `FORMAT_KEY` is written to the index and asserted by `--check`; fake-S3 test. |
| C#1 | P2 | Per-tile mask cost up to 40 ms on fjord tiles (Bergen z8, cell 60_5 with 83k vertices; Baltic tier 0 up to 19 ms) against the plan's ≤ 10 ms typical. | `landPathsForTile` collapses runs of consecutive vertices that all lie beyond the same side of the tile to their end points (a segment between two points of one half-plane never enters the tile, so the winding inside is unchanged), with the decimation judged against the last decimation survivor so the collapsing cannot change which vertices go; test pins mask equality with the uncollapsed reference at four zooms (a z6 tile inside a 20k-vertex ring keeps 8 vertices). (2.6.3) |
| A5 | P3 | In-flight bookkeeping after `abortAll()` could lose a record and exceed the bound. | `inflight[name]` holds the controller; a handler only clears its own record (`mine()`). |
| A6 | P3 | `setCoast(null)` (wind) left `onChange` bound to the layer. | `setCoast` detaches `onChange`/`onNeeded` when they are this layer's; test. |
| A7 | P3 | Index/data trusted beyond validation (`cell: 0` hung the tab; unbounded longitudes). | `validCoastIndex` (format, `cell` > 0 ≤ 90 with 180 % cell = 0, `dir` `[A-Za-z0-9_-]+`, `cells` a non-array object) and decoder bounds (|lon| ≤ 180 + 1/q, |lat| ≤ 90 + 1/q); tests. |
| A8 | P3 | After a failed coast load every later mount blocked the first frame for up to 15 s again. | `failedAt`: within 60 s of a failure `load()` resolves `null` at once (unclipped + warning); test. |
| A9 / C#3 | P3 | The decoded chunk LRU (≤ 32 MB) survived Off. | `abortAll()` clears the chunks (tier 0, 2.2 MB, is kept for a fast re-mount); README states the retained set. |
| A10 | P3 | The request queue was never trimmed to the view. | `_pump` keeps only cells the layer still needs (`onNeeded`); test. |
| A11 | P3 | Test gaps, one test pinning A1's wrong behaviour. | Tests added for A1, A2, A3, A4, A5, A6, A7; the two-zoom `readoutAt` case remains open (nit). |
| B P3-1 | P3 | `normalize_ring`'s ±180 shift/split decision rides on cumulative-sum rounding. | A snap to exactly ±180 was added in 2974ef9 and **withdrawn in c3ca20d**: it changed one byte in cell -80_175, so the builder no longer reproduced the published v1 objects and the new guard would have refused the licence publish. Kept for the next data build (new prefix); the published build is correct (B's verification). |
| B P3-3 | P3 | `--check` gaps. | Per-cell vertex counts vs the index, world land area within 2 % of 22,100 sq deg, eleven land/water landmark probes, `format_key` (gated by `WORLD_MIN_CELLS` for synthetic worlds). Per-cell sha256 in the index and the empty-file/over-long-varint hardening remain open. |
| B P3-4/5 | P3 | Tier hand-over facts (islands < ~15 km² appear at z7; tier 0 ≈ 1 px at z6 above 60° N). | Documented in `tools/coast/README.md`; accepted. |
| B P3-6 | P3 | Workflow hygiene. | `redact()` around upload errors; `client-tests` job runs the Node suite; push paths include `static_overlay/**` and `tests/overlay/**`. The checkout pin 11d5960a **is** the v4.4.0 tag (GitHub API `refs/tags/v4.4.0` → that commit), comment corrected; the 9-minute serial upload is accepted. |
| B P3-7 | P3 | Builder test gaps. | Added: real pole-ring storage (first 180, last -180) and a 0..360 Greenwich ring, the seam invariant after quantisation, `check()` catching a clockwise ring / count mismatches / a piece outside its cell / a wrong `format_key`, `upload()` against a fake S3 (order, headers, refusal, `--replace`), `redact`, reader guards. Open: `split_cells` tangent/on-line/full-cell cases, a cross-language fixture. |
| B P3-8 | P3 | Documentation drift; bucket lifecycle rule unknown. | README hand-over facts added; plan §19 marks the C1 RESULTS as authoritative for the built layout. Lifecycle rules: see "Fix round" below. |
| B P3-2 | P3 | ≤ 1-unit (≤ 110 m, ≤ 0.15 px at z11) quantisation ledges on 69 of 2,946 cell boundaries. | Accepted as documented. |
| C#4 | P3 | Frame draw 39/56 ms avg/max at z8 on the review machine (pre-existing bilinear sampler cost, 0 mask recomputes over 58 frames). | Accepted; not a clip cost. |
| C#5 | P3 | Layout on maps under ~200 px tall (the sheet and the lifted attribution cover markers). | Pre-existing since G4; backlog. |

## Verified correct by the reviewers (evidence in their reports)

- A: real data decodes with counts equal to the index; per-tile cost ≤ 3.4 ms at z1–11 (scanline, Node);
  adjacent tiles agree on shared edges; the dateline and world copies produce identical masks and keys;
  `readoutAt` uses Leaflet's unwrapped `x:y:z` keys and `Math.round(zoom)`; `composeTile` alpha 127/128
  sits exactly on the `LAND_READOUT` line; the store holds ≤ 2 in flight with correct LRU order; hostile
  files (6-byte varints, trailing bytes, truncation, > 8 MB) rejected; no `innerHTML`/`eval`.
- B: GSHHG header/flags decoded as documented; all 1,473 cells decode, every piece one CCW ring inside
  its cell; tier-1 land area 22,102 sq deg (tier 0 22,093); 0 mismatches in 24,000 random points over
  16 regions incl. Greenwich, the dateline and Antarctica; 0 self-intersections in 3,150 rings; the JS
  and Python decoders agree on 8 published files; R2 ETags of all 1,474 objects equal the local build;
  headers immutable + nosniff, `.bin` edge HIT, `index.json` DYNAMIC, CORS allow-list correct; `prune`
  cannot touch `static/`.
- C: tier switch 6→7 over Oahu final in 670 ms without a visible snap; chunk requests only for cells
  the tiles touch, ≤ 2 in flight; playback 1× 501–520 ms, 4× 126–148 ms with 0 mask recomputes;
  markers, units, controls, Off teardown (0 requests after Off), rapid field switching, 5 Off/On cycles,
  phones, the coast-unreachable path (unclipped + warning after the 15 s watchdog) all as designed;
  first display 933 ms.

## Fix round (author, MAX effort)

- The client fixes were verified by the Node suite (43 tests) and pytest (368); the collapsing in 2.6.3
  is pinned exact by mask equality at four zooms.
- Coast-build run #6 (dispatch from `feat/overlays-coast`, upload=true, to publish `LICENSE.txt`) went
  **red at "Build and check"** — the P2-1 fix working — because the Bergen landmark probe added for
  B P3-3 (5.3 E 60.39 N) is fjord water in GSHHG; a local rebuild reproduced it. The probe now sits on
  Ulriken (5.386 E 60.377 N). The same rebuild showed the B P3-1 snap changing one byte in cell -80_175
  (1,342 → 1,341 bytes); withdrawn so the builder reproduces the published build byte for byte
  (confirmed by a local rebuild against the published index and the published `-80_175.bin`).
- Bucket lifecycle rules (B P3-8): checked in the Cloudflare dashboard — the bucket carries only the
  default multipart-abort rule (incomplete multipart uploads aborted after 7 days) and no bucket lock
  rules; nothing expires objects, so `static/coast/v1/` persists.

## Licence publish

coast-build run #9 (dispatch from `feat/overlays-coast` @ c3ca20d, `upload=true`, 2026-09-24 22:02 UTC):
tests + client-tests green, build + `--check` clean (0 problems, format key asserted), the upload guard
recognised the published build (tier-0 sha256 aabbff51…, all 1,473 cells identical) and wrote only
`LICENSE.txt` (new, 10,657 B) and a refreshed `index.json` (now carrying `format_key`; same cells and
hashes), then read the index back. Verified from outside: `LICENSE.txt` answers 200 `text/plain`,
`public, max-age=31536000, immutable`, nosniff, and begins with the modification notice; no other
object was touched. Run #12 (22:42 UTC, builder bc59ed2) repeated the same-build refresh: `--check`
clean with both dateline probes evaluated at tier 1, `LICENSE.txt` now dated (10,746 B), `index.json`
now carrying `tier1.sha256` (a4b97403…, equal to a local rebuild's), same tier-0 hash and cell map.

## Asset 2.6.3 on the test site (author, 1280×800 Browser pane, wave-app-clean @ test 22f0284)

Asset served immutable + nosniff with sha256 = the repo fixture; page `VERSION = "2.6.3"`. Readout =
drawn-pixel invariant (every current-zoom tile, every 3rd/4th pixel, `readoutAt` non-null iff composed
alpha ≥ 128 and equal to `_value(tileCodes)` at that pixel):

| View | tiles (mixed/ocean/all-land) | samples | mismatches | mask compute avg / max |
|---|---|---|---|---|
| Oahu default, zoom 6 (tier 0) | 18 (5/13/0) | 73,728 | 0 | — |
| Oahu 21.48 N 157.97 W, zoom 8 (4 chunks) | 18 (8/10/0) | 133,128 | 0 | 0.41 / 7.5 ms |
| Bergen 60.39 N 5.32 E, zoom 8 (60_5, 60_0, 55_0, 55_5) | 18 (8/6/4) | 73,728 | 0 | 0.70 / **10.2 ms** (tile 131/74: 9.2 ms, was 40.3 in C's review) |
| Bergen zoom 10 | 18 (13/4/1) | 73,728 | 0 | 0.93 / 5.2 ms (was 15.6) |

The caption carries the credit "coastlines from GSHHG (Wessel & Smith), LGPL" as a link to
`https://models.allshoresurf.com/static/coast/v1/LICENSE.txt` (`rel="noopener license"`, new tab), no
`ov-warn`; chunk requests only for the cells the tiles touch, all edge-cached on the second visit. Off:
layer null, model pane gone, 0 tiles, the chunk cache emptied (A9) with tier 0 retained, 0 requests in
the 4 s after Off, no console errors.

## Re-review of the fix set (fresh context, MAX; `abd0791..c3ca20d`)

Report: scratch `g5-rereview.md`. **0 P0, 1 P1, 1 P2, 6 P3.** It confirmed every G5 fix with its own
evidence — A1 for 404/410 (stand-in, final, no re-request), the A2 timer semantics (one handle, re-arms,
cleared on Off, cannot fire after Off, a new mount binds fresh), A3 with an exact signed-area rasteriser
on real data (0 land pixels lost on every cell line tested; abd0791 lost up to 118/255), the 2.6.3
collapsing exact on 1,516 cases incl. 1,500 fuzzed self-intersecting ring sets (winding equality and
identical masks), A4–A10, the licence texts verbatim from the archive and published byte-identical, the
workflow guard, the action pins resolving to their tags, and builder c3ca20d reproducing the published
bytes (the withdrawn snap only rotated one ring's start vertex in `-80_175`: same vertex set and area).
Two of my fixes had introduced regressions:

| # | Sev | Finding | Fix (asset **2.6.4** @ e7e35b6, builder @ bc59ed2) |
|---|---|---|---|
| R1 | P1 | The A10 queue trimming ran while Leaflet was still creating a tile (`_addTile` calls `createTile` before setting `_tiles[key]`), so the cells only that tile needed were dropped as "no tile wants it"; a pan revealing a lone tile on a new cell stayed on the 1 km stand-in with nothing scheduled (reproduced at Niihau with real data). | `request()` marks the names it was just asked for and `_pump(fresh)` never trims those; stale entries are still trimmed at the handlers' pumps. Test: `createTile` before registration fetches both cells. |
| R2 | P2 | The in-flight record was released before `decodeCoast`, so a 200 with a bad body was never recorded: re-fetched on every re-evaluation, and the queue stalled when both slots hit bad cells. | Decode while the record is still ours (any throw → 'coast decode failed' → failed for good). Test: a corrupt chunk is failed once, `rev` moves, the queue keeps moving. |
| R3 | P3 | `validCoastIndex` still admitted `cell: 2^-10` / `max_zoom: -1` (a 67-billion-name enumeration). | Integer `cell` ≥ 1 and integer `max_zoom` ≥ 0; tests. |
| R4 | P3 | `same_build` could not see a content change that keeps every file's length (demonstrated with the fake S3). | `tier1.sha256` = one hash over every cell file's sha256 by name, written by the build, verified by `--check`, compared by the guard whenever both indexes carry it (the published index gets it at its next same-build refresh). |
| R5 | P3 | The pole-ring test became vacuous once the snap was withdrawn. | It now asserts the builder's actual behaviour (near-dateline vertices kept as stored, quantised onto the dateline). |
| R6 | P3 | The Ross Sea probe at lon 180 was never evaluated at tier 1 (no cell starts at 180). | Probes at ±179.9999 (cells 175 and -180). |
| R7 | P3 | The modification notice gave its date by reference. | The notice states 2026-09-24; republished with the index refresh. |
| R8 | P3 | This record was still the placeholder at c3ca20d. | Filled in and committed before the merge. |

Also noted by the re-review, accepted: a mixed tile with one unservable cell draws entirely from tier 0
(the documented design); `setCoast(otherStore)` would serve a mask keyed on z/x/y for a different store
(unreachable: one store per URL).

Asset 2.6.4 on the test site (test @ 899516a, 1280×800 pane): served immutable with sha256 = the fixture,
page `VERSION = "2.6.4"`; zoom-6 Oahu readout = drawn pixel at 133,128 samples with 0 mismatches; the R1
scenario replayed live — chunk cache emptied, then `createTile` for the Niihau tile (zoom 9, x 28, y 224)
before any registration put both of its cells (20_-160, 20_-165) in flight at once and both were cached
3 s later with no failure; Off left layer null, 0 tiles, the chunk cache empty and 0 requests; no
console errors. (The mask path is unchanged from 2.6.3, whose timings above stand.)

## Second focused re-review (fresh context, MAX; `c3ca20d..bc59ed2`)

Report: scratch `g5-rereview2.md`. **0 P0, 0 P1, 1 P2, 5 P3.** R1 and R2 confirmed fixed under Leaflet's
real `createTile`-then-register order (read from the 1.9.4 source), a fetch resolved before `createTile`,
stale trimming, cooldowns, `abortAll`, and 400 randomised sessions (no registered tier-1 tile left on the
stand-in, in-flight never above 2); every unusable 200 body (corrupt, empty, oversize, truncated) recorded
once with the queue moving; `validCoastIndex` rejects every listed bad value and the published index
validates; the builder's content hash verified end to end, including the exact production refresh against
the real published index (only `LICENSE.txt` + `index.json` written, read back accepted); all probes pass
at both tiers. Findings and the third fix round (asset **2.6.5** @ 00bd899, builder @ 8ff8a80; 46 Node + 368 pytest):

| # | Sev | Finding | Fix |
|---|---|---|---|
| N1 | P2 | A well-formed coast-v1 body encoded for ANOTHER cell (or a tier-0 file) served under a tier-1 name was stored as that cell, and the tile then drew no land at all, for good. Pre-existing; unreachable today (the builder's `--check` refuses a piece outside its cell before any upload, objects are immutable, and every published object equals the checked build), hence P2. | A decoded chunk must carry the index's cell size and keep every piece inside the named cell (`withinCell`, half a world pixel of Float32 slack for the pole rows), else it fails for good like a corrupt body. Tests serve per-cell bodies; new test with 60_20's land under 20_-160 and a tier-0 header. |
| N2 | P3 | A hostile-but-valid index (`cell: 1`, all 64,800 cells) cost ~1 s per tile per arrival (`indexOf` per name, re-evaluation of every pending tile). | Queue membership map; a tile touching more than 16 cells draws from tier 0 for good; `index.json` over 1 MB is not parsed. |
| N3 | P3 | The ±179.9999 probes at 74 S gated into cells that hold no file (open Ross Sea): tier 1 still not polygon-tested at the seam. | Probes at 76 S (water) and 79 S (Ross Ice Shelf front, land) inside cells -80_175 / -80_-180; verified at both tiers on the published build. |
| N4 | P3 | The quantisation half of the pole-ring test was still vacuous. | It asserts the noise vertices quantise onto the dateline. |
| N5 | P3 | `same_build` was symmetric: a hash-less LOCAL index against a hashed published one passed (an older checkout could strip the hash). | The local index must carry the hash. |
| N6 | P3 | This record was still the placeholder in the reviewed range. | Committed with the fixes. |

2.6.5 on the test site (test @ d3e9dca): served with sha256 = the fixture; the real tier-1 chunks pass the
new cell guard (Oahu z8: 4 chunks, 18 tiles final, 133,128 samples, 0 mismatches; Bergen z8: 8 chunks,
0 mismatches, no failures); Off clean with 0 requests after Off; no console errors. The all-chunk sweep
in Node then showed the half-pixel slack accepting the pole cell's bytes under its neighbour's name
(the two rows nearest a pole are 0.43 world px tall at zoom 0): asset **2.6.6** tightens the slack to
0.01 px — verified over all 1,473 built chunks (byte-identical to the published ones): 1,473 accepted,
20 of 20 neighbour swaps refused; 46 Node + 368 pytest.

2.6.6 on the test site (test @ ee7a35d): served with sha256 = the fixture; Oahu z8 — 4 real chunks
accepted, 18 tiles final, 133,128 samples, 0 mismatches; the polar dateline (Ross Sea 77.5 S 179.5 E,
zoom 7) — chunks -80_175, -80_170, -80_-180, -80_-175 accepted by the tightened guard, 12 tiles final,
49,152 samples, 0 mismatches; Off clean with 0 requests started after Off; no console errors.

**G5 closed.** Client asset 2.6.6 (`feat/overlays-coast` @ 01f5ac8 + this record) and the published
coast data v1 are ready for the production merge (`git merge --no-ff` into `Live-Buoy-Update`, rollback tag
`prod-pre-coast` @ c6926e7 first).
