# Overlays — G4 pre-production review

Date: 2026-09-23 04:25–05:05 UTC. Scope: `feat/overlays-2` @ 3cee801 (asset v2.5.0; v2.5.1 landed on the test site during
the review) against production `Live-Buoy-Update` @ 92be745; the R2 custom domain `models.allshoresurf.com` created the
same night; the frame job as merged (schema 3, cron every 10 min). Three fresh-context reviewers at max effort: **A**
security (DNS move, bucket exposure, site surface, browser module, workflows), **B** the full §7 regression list and §6
budgets on the test site in the Browser pane (desktop and phone), **C** production readiness (diff audit and byte identity,
live production baseline, rollout/rollback, job health, suites, docs).

**No P0 in the code. One P0 procedural (C1), three P1 (B1 code; B2 and C2 operational), two P2, many P3.** Code items are
fixed in assets v2.5.1 (A) and v2.5.2 (B); the operational items are in the rollout plan below.

## Findings and disposition

| # | Sev | Rev | Finding | Disposition |
|---|---|---|---|---|
| 1 | P0 (procedure) | C | `feat/overlays-2` predates the two job-only merges (bbfa2cd, 92be745); pushing the branch over production would have reverted the schema-3 job and the 10-min cron | Production receives `git merge --no-ff feat/overlays-2` (merge-tree conflict-free, exactly the 15 site files); README says so; tag `prod-pre-overlays` at 92be745 first |
| 2 | P1 | B | Retry after an outage with the last frame still cached left the panel on "Loading…" for ever (the cached-frame shortcut in `_goto` skipped the ready transition); the outage counter also stayed at 3 | v2.5.2: the shortcut performs the same state transition without a redraw; Retry resets the counter; a seek/step reaching the outage threshold shows the Retry state like playback; Node test |
| 3 | P1 (rollout) | B, A | Resolvers that still hold the old GoDaddy delegation answer NXDOMAIN for `models.allshoresurf.com` until the parent NS TTL runs out (≈ 2026-09-25 04:00 UTC); the pane lost the name for ≈ 60 s during the review | Production's frames URL is not switched to the custom domain before that time (or until the owner's own resolver returns the Cloudflare nameservers); the code push with the flag unset is unaffected |
| 4 | P1 (job) | C, B | GitHub dropped ten of twelve overnight `7,37` ticks and had not fired the `*/10` schedule 50 min after the merge; the 18Z cycle went live 7.1 h after its run time | Manual dispatch (run #6) published the 00Z cycle at 05:02 UTC and ran the legacy prune; the schedule stays under observation and manual dispatch is the interim; flag-on on production waits for an observed scheduled run |
| 5 | P2 | A | A hostile PNG could be inflated ≈ 1000× before `validateGrid` rejected it (bucket-compromise scenario) | v2.5.1: bodies over 2 MB and pictures whose IHDR size is not the manifest grid are refused before any inflate; the canvas path checks the bitmap size; Node test |
| 6 | P2 | B | Heap during playback: live set +13 MB, GC peaks +34 MB at 1× (+45 at 4×) against the "+25 MB" line; halved from v2.4.0 | Accepted as measured: the budget is restated as live set ≤ +25 MB with GC peaks ≤ +50 MB (§18); the remaining garbage is the 1 MB inflate output per frame |
| 7 | P3 | A | `/overlay/*` answered a JSON 404 while off where the site answers an HTML 404 | v2.5.1: `abort(404)` → the site's default 404, tested byte-equal |
| 8 | P3 | A | `_effective_tz_name` returned the raw `?tz=` string (pytz accepts spellings V8 rejects) | v2.5.1: the canonical IANA name; tested |
| 9 | P3 | A | `validateManifest` left `complete`, `run_utc`, `model` and the frame count untyped/unbounded | v2.5.1: typed, ≤ 512 frames; tests |
| 10 | P3 | A | Asset version discipline was manual | v2.5.1: `tests/fixtures/overlay_assets.json` pins the assets' sha256 to `OVERLAY_ASSET_VERSION`; a change without a bump fails the suite |
| 11 | P3 | A | Plain HTTP served objects on the frames hostname; bucket responses had no `nosniff`/CSP | Zone: "Always Use HTTPS" on (proxied hosts only, so the site's DNS-only records are untouched); Response Header Transform rule on `models.allshoresurf.com` sets `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox; default-src 'none'`; verified by curl (301 on http, both headers on https) |
| 12 | P3 | B | A stalled frame download (one 155 KB frame took 6.5 s) froze playback with no deadline | v2.5.2: a download slower than 5 s (or 4 intervals) is aborted and treated as a transient failure; playback skips it |
| 13 | P3 | B | Short desktop windows (map ≈ 305 px) got a slit of details above the zoom stack | v2.5.2: maps under 330 px use the sheet |
| 14 | P3 | A | 403 from a bot check is transient (already so since G3); `Python-urllib` and `libwww-perl` user agents get 403 from Cloudflare's browser integrity check on the frames hostname | Noted for monitoring scripts (use a browser UA or curl) |
| 15 | P3 | A | Failure records in the bucket may embed the R2 endpoint URL from botocore messages; `prune_legacy` is permanent rather than one-shot; `--force` now leaves stale frames in the edge cache; conda packages unpinned; `setup-python@v5` unpinned in the test workflow | Job-side follow-ups for the next job-only push (redact URLs, restrict `prune_legacy` to the two legacy keys, pin package versions and the action SHA); `--force` is an operator note in the README |
| 16 | P3 | C | Rollback rung 4 ("disable the workflow") was undone by the keepalive workflow; the `prod-pre-overlays` tag did not exist; plan §6 not revised after the wind decision; stale plan text | README: disable both workflows; the tag is taken at push time; plan §18 restates the budgets and the status |
| 17 | P3 | B, A | `latest.json`/manifest are `cf-cache-status: DYNAMIC` (JSON not in Cloudflare's default cache list) — one 5.7 KB origin read per session | Accepted; the pointer's `max-age=300` keeps it fresh |
| 18 | P3 | B | Phone attribution wraps to two lines (carried from G3) | Accepted |

## Verified by the reviewers (evidence in their reports)
- **Byte identity with the flag unset** (C, A): the five golden scenarios render identically from production, the feature
  branch, the feature branch with the frames base set, and the merge result (496,754 B total, per-scenario sha256 equal);
  the golden equals a fresh capture from production; the only route difference is `/overlay/<name>`; `after_request`,
  error handlers, Jinja options and config identical; no `static/` folder in either tree; requirements unchanged.
- **Live production baseline** (C, 04:28 UTC): six probes recorded (sizes, TTFB, cache headers), zero overlay markup.
- **DNS move** (A): every record answers identically from Cloudflare and GoDaddy's servers; the site is served by Render
  directly (Render's own certificate and headers); DMARC intact; no MX; no DS at the parent; delegation at the registry
  already points at Cloudflare. Bucket: listing impossible, writes 401, CORS only for the three allowed origins (edge cache
  keyed by Origin), immutable frames, `max-age=300` pointer, no sensitive objects. Browser module: no eval/innerHTML, all
  manifest strings as text, fetch targets confined to the configured host, storage type-checked, no prototype pollution.
  Workflows: `contents: read`, secrets only as step env, actions pinned by SHA in the publisher.
- **§7 regression on the test site** (B): 13 of 14 PASS on v2.5.0/2.5.1; item 9 (bucket blocked mid-session) FAILED only
  on Retry (finding 2, fixed in v2.5.2). §6: first display 250–382 ms (cold ≈ 0.3–0.6 s); cadence p50 506 ms at 1× and
  140 ms at 4×; ring ≤ 5, in-flight ≤ 2; second loop 0 bytes; desktop loops Hs 11.9 / Tp ≈ 12.5 / wind 9.9 MB; phone
  loops Hs 4.3 / Tp ≈ 4.5 / wind 9.9 MB; heap after Off back to baseline; 0 origin requests per frame; every model
  request went to `models.allshoresurf.com` (none to r2.dev); edge cache HIT on repeat requests.
- **Suites** (C): 344 pytest + 24 Node on the branch, 346 pytest + 34 job tests on the merged tree (before the v2.5.1/2
  additions: now 345 pytest + 27 Node on the branch).
- **Job health** (C): 5 runs in 24 h, none failed; the live pointer complete; retention intact; the pre-v1 objects were
  pruned by run #6 (the custom domain still serves one edge-cached copy of a deleted frame, harmless).

## Rollout plan (operator steps)
1. `git tag prod-pre-overlays 92be745 && git push origin prod-pre-overlays` (rollback rung 2).
2. `git checkout Live-Buoy-Update && git pull --ff-only && git merge --no-ff feat/overlays-2` → `git diff --stat HEAD~1`
   must list only the site files; run `pytest tests` and the two Node files; `git push origin Live-Buoy-Update`.
3. Verify production with the flag unset: the page has none of `ovField`, `AllshoreOverlay`, `/overlay/`, `modelPane`;
   `/overlay/overlay.js?v=<version>` → 404 with the private default cache policy; the six baseline probes match modulo
   live data; the golden test passes against the deployed HTML.
4. Wait for (a) an observed scheduled run of the frame job and (b) the nameserver window (≈ 2026-09-25 04:00 UTC, or the
   owner's resolver returning the Cloudflare nameservers); then set on the production service
   `MODEL_OVERLAYS=1` and `MODEL_FRAMES_BASE=https://models.allshoresurf.com/gfswave/0p25/v1`, and verify: selector
   present, assets 200 immutable + nosniff, a frame renders, the Off page still makes zero bucket requests.
5. Afterwards: disable the r2.dev public URL; keep the rollback ladder (unset the flag; re-push the tag; bump the asset
   version; disable BOTH workflows).
