# Overlays — G6 adversarial review (coastal fill in the frame job) — 2026-09-24

Gate G6 of plan section 19 ("Coastline boundaries"), before the job-only merge of the coastal fill.
Reviewed on `feat/overlays-fill` @ be7fbe1 (one commit on production `Live-Buoy-Update` @ 8ee00e1).
The browser clip (asset 2.6.6) is already live, so the fill is only ever seen where the client does not
clip: over water.

What the commit does: `tools/model_frames/encode.py` `fill_coast` gives every empty GFS-Wave cell within
4 cells of model data the mean of its present 8-neighbours (one Jacobi pass per ring, longitude periodic,
nothing beyond the poles) for wave height and peak period, before quantisation and before the half
subsample; model values never change; wind is never filled. The manifest carries a `fill` block, the
stats sidecar `filled_points`, and `run.main` refuses to publish a run whose published manifest was built
with a different fill (frame keys are immutable).

Author's measurements before the review:

| Measurement | Value |
|---|---|
| Live run 2026092418 f000, empty cells (both fields) | 449,175 before, 356,776 after (92,399 filled) |
| Model codes changed by the fill | 0 |
| Central Oahu node | empty before; 1.71 m and 7.96 s after |
| Fill time per grid (local) | about 0.2 s |
| PNG growth at f000, full / half | hs +11 % / +9 %; tp +15 % / +13 % |
| GitHub dry run #56 (branch, steps 0 and 3) | success; 2.1 s and 1.8 s per step incl. fetch and decode; `filled hs=92399 tp=92399`; max RSS 179 MB |
| Filled cells by latitude | 32,432 north of 60 N, 23,688 south of 60 S, 36,279 between |
| Filled cell centres over GSHHG water (300-cell samples) | 36 % north of 60 N, 72 % south of 60 S, 10 % between |

The polar share is most likely sea ice, which the model masks and the client does not clip: the known
risk the plan accepted ("ICEC exclusion is a follow-up"). Reviewer A was asked to verify and size it.

Reviewers (fresh context, no access to the author's reasoning):
- **A — numerics**: `fill_coast` semantics, the half-grid coverage argument, Tp and Hs means, the ice edge,
  stats invariants, tests.
- **B — operations**: manifest `fill` vs the live client, the re-publish guard, budgets from a real run,
  rollback and retention, tests, hygiene.

## Findings

**Outcome: 0 P0, 1 P1, 2 P2, 9 P3.** Reports: scratch `g6a-report.md` (A) and `g6b-report.md` (B).
Fixes in 73fa040 on `feat/overlays-fill`.

| # | Sev | Finding | Fix |
|---|---|---|---|
| A P1-1 | P1 | The fill painted about 25,700 cells (5.8 M km2) of invented waves over the sea-ice pack the model masks, more than one cell from any land (9,964 north, 15,745 south on run 2026092418 f000; the pack is riddled with model water cells, so the fill closed much of its interior, far beyond the plan's "1 degree past the ice edge"). The browser never clips it (sea ice is not GSHHG land). | The fill writes only where the node's 0.25-degree box, or a neighbour's, holds GSHHG land: `tools/model_frames/fill_allow.png` (12 KB, 406,695 allowed nodes) built by `make_fill_mask.py` from the published coast v1 (tier-1 hash in its text chunk), sha256 pinned in `encode.py` and checked on load. Filled cells 92,399 → 66,661; ice bleed 0 (A measured coast coverage 98.483 % → 98.476 %). |
| A P2-1 | P2 | Averaging Tp blended swell regimes into periods no nearby model cell has (Panama isthmus 8.6–12.5 s between 6.9 and 14.4 s; up to 12 s off the nearest model cell at 7.6–9.3 % of filled coast points), while the client samples Tp by nearest node. | Tp takes the value of the nearest model cell (Euclidean, fixed tie order; same filled set as the mean). Every filled Tp is a model value; Panama → 6.9 / 14.4 s; central Oahu 8.76 s. Hs keeps the neighbour mean (0.08 % of filled cells > 0.5 m from every model value within 4 cells). |
| B P2-1 | P2 | If the coastline data cannot be loaded, the client draws hs/tp unclipped; with the fill that now covers land up to ~4 cells inland, and the "extrapolated" sentence is dropped in that case. | **Accepted for now, client follow-up offered**: the path already shows the warning "Coastline data could not be loaded; the field is shown without coastline clipping", needs the coast files to fail while the frames (same host) load, and self-heals after 60 s. A client change (hide hs/tp or keep the sentence when `fill` is present and the clip is unavailable) needs its own asset release. |
| B P3-1 | P3 | The guard failed every tick (exit 2) when a run had a complete manifest but the pointer write had failed, or `latest.json` was lost. | `run.fill_guard` re-points `latest.json` to that run's existing complete manifest (only when it is newer than the live run, never a regression, never a partial) and exits 0; refusals remain for forced rebuilds. |
| B P3-2 | P3 | `git revert` of the fill commit also removes the guard, so a forced rebuild could rewrite filled runs' immutable frames. | Rollback is now documented as setting `"fill": False` for hs and tp (one line; the manifest then omits `fill`, the guard stays and protects filled runs) — docstring and README. |
| B P3-3 | P3 | The refusal and the fill counts went only to logs; exit 2 undocumented. | Step-summary lines for the fill counts, refusals and pointer repairs; exit codes documented. |
| B P3-4 | P3 | An unreadable manifest crashed the job before the failure record. | `publish.newest_manifest` + a clean refusal (exit 2, summary warning). |
| B P3-5 | P3 | Wording changes counted as a different fill. | `encode.fill_key` = (version, fields, cells); `FILL_VERSION = 2`. |
| B P3-6 | P3 | Test gaps (dry run touching the bucket asserted only in a comment; partial-only manifest; reordered keys; matching fill under `--force`; `--allow-partial --force`). | Tests added for each. The test workflow's numpy/Pillow pins differing from the job's (pre-existing) remain open. |
| B P3-7 | P3 | Budgets: loops grow hs +9–11 %, tp +14–16 % with the unmasked fill; the job gains tens of seconds per run. | Re-measured after the fix: tp full PNG at f000 +6 % (was +15 %), hs +9 %; dry run #60 on the branch 4.3–4.5 s per step (was about 2 s), about +3.5 min per 81-step run against the 60-minute timeout; max RSS 203 MB. The owner has said size is not a constraint. |
| A P3-1 | P3 | The docstring, commit message and plan rested on two false premises (GFS-Wave blanks every land-touching cell; 4 passes is the minimum that reaches every coastline). | The docstring states the real rule (a land-fraction threshold) and the coverage lemma: after k rings a coast point is drawn on full frames when its nearest node is within k cells of model data, on half frames within k − 1; 4 is a reach/extrapolation trade-off; no k covers the Black Sea (no model cells). |
| A P3-2 | P3 | The pole test cannot tell "nothing beyond the pole" from a correct across-pole neighbourhood. | Noted; no effect (the model has no cells north of row 9 or south of row 674; the client never samples beyond ±85.05°). |

Verified correct by the reviewers: Jacobi semantics and periodic columns against a brute-force reference;
model values and the input untouched; `nonzero codes = valid + filled` and `half = full[::2, ::2]` on real
frames; the live client (2.6.6) accepts the new manifest (5,932 B) and runs of both kinds coexist with the
right caption; the guard covers every path that writes frames; an "already live" tick costs no extra
request; wind codes unchanged; no secrets in logs.

## Fix round verification (author)

- 47 job tests, 378 in the full suite.
- Live run 2026092418 f000 locally: 66,661 filled cells for both fields, model codes unchanged, none more
  than one cell from GSHHG land; every filled Tp a model value.
- Dispatch #60 (branch @ 73fa040, steps 0/120/240, dry run): success; `filled hs=66661 tp=66661` at every
  step (the pinned mask loads and verifies on Linux); max RSS 203 MB.

## Re-review of 73fa040 (fresh context)

Report: scratch `g6-rereview.md`. **0 P0, 0 P1, 1 P2, 6 P3**; verdict: fit for the job-only merge, P2 fix
recommended before the first filled run. It rebuilt the mask independently (406,695 nodes, 0 differ;
box alignment, pole rows, dateline and a Pillow-free decode checked), found 0 ice bleed and coastline
coverage unchanged (hs 98.476 % full / 98.629 % half; Hawaii 100 % for both fields at both resolutions),
traced every guard path (the repair never regresses the pointer or names a partial; `point_to` writes the
same bytes as a publish; frames are byte-deterministic, so a same-fill rebuild is safe) and confirmed the
rollback behaviour. Fixed in the final commit:

| # | Sev | Finding | Fix |
|---|---|---|---|
| R-P2-1 | P2 | Tp "nearest" searched a Chebyshev-4 window; the Euclidean-nearest model cell can be 5 cells straight across (d 5.0 < the window corner's 5.66), so 1.8 % of filled Tp cells took a farther cell (1,232 different values, up to 12.9 s). | Window `floor(cells * sqrt(2))` = 5, which contains every model cell within the Chebyshev-4 guarantee's Euclidean bound; equals a window-8 brute force on the live f000 (66,661 cells), 0.8 s per grid; brute-force test on a random grid plus a pinned beyond-the-corner case. |
| R-P3-1 | P3 | "never over open water or sea ice" overstated: up to one cell (~28 km) of fill over coastal sea ice remains (about 1,400 all-water nodes at Antarctica now, more in the northern winter). | Manifest `limit`, docstring and README now say so. The GFS ICEC exclusion stays a follow-up. |
| R-P3-2 | P3 | The guard key omitted the method and the mask. | `FILL_INFO["mask"]` = the mask's hash prefix; `fill_key` = (version, fields, cells, methods, mask); tests. |
| R-P3-3 | P3 | The pointer repair did not validate the manifest (a foreign one would be pointed to; one without `encoding` crashed). | Repair only when `run`, `encoding`, `complete is True`, 81 frames and `published_utc` check out; otherwise refuse; tests. |
| R-P3-4 | P3 | A transient R2 error in the guard exited 2 like a refusal. | Content errors exit 2, transport errors exit 1 (retried next tick); test. |
| R-P3-5 | P3 | The documented rollback turned six tests red and did not mention the up-to-6-hour tail. | Docstring and README: adjust the fill tests in the same commit; the filled live run stays until the next cycle unless `latest.json` is pointed at an older run by hand. |
| R-P3-6 | P3 | Test gaps (no brute-force nearest test, no manifest-without-`fill` test, no mask-regeneration test). | The first two added; the mask regeneration needs the 22 MB coast data and stays a manual step (`make_fill_mask.py`, verified independently by this re-review). |

Final state: 380 tests (49 job). **G6 closed**; the job-only merge waits for the owner's go-ahead.

## Owner-requested check before the merge (2026-09-25)

The owner saw the old gaps on the test site; expected, since no filled run has been published (the test
site reads the production bucket, and the branch has only run as dry runs). To verify the fill end to end
with the real client, the site (client 2.6.6) was run locally twice against two local copies of the bucket
for run 2026092418: the published hs/tp frames as-is, and the same frames passed through the branch's
`fill_coast` (model codes unchanged; 66,661 cells filled per frame). Per view, every current-zoom tile was
read back and each water pixel of the coastline mask checked for colour:

| View | Wave height: uncoloured water px, published / filled | Peak period: published / filled |
|---|---|---|
| Oahu z9 | 16,855 / 0 | 50,862 / 0 |
| Oahu z11 (Kaena Pt) | 49,118 / 0 | 170,277 / 0 |
| Kauai z9 | 10,655 / 0 | 33,539 / 0 |
| Maui Nui z9 | 10,477 / 0 | 38,004 / 0 |
| Big Island z8 | 3,151 / 0 | 13,636 / 0 |
| Hawaii z6 | 269 / 0 | 972 / 0 |
| San Francisco Bay z9 | 16,816 / 0 | 22,620 / 0 |
| Bergen z8 | 33,004 / 4,406 | 36,441 / 6,468 (inner fjords > 1 degree from model water) |
| Tokyo Bay z8 | 10,950 / 0 | 26,233 / 0 |

No land pixel was coloured in any view; readout = drawn pixel at 73,728 samples (Tp, Oahu z9); the caption
carries the "extrapolated" sentence. A before/after sheet on the site's imagery (Oahu to Maui, zoom 9) was
sent to the owner.
