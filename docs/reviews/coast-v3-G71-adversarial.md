# G71: adversarial review of coast set v3 (plan section 104, release 1)

Reviewed: feat/coast-v3 at 203db4f (d4c4c54 + 203db4f), against production 1609afd. One fresh reviewer, read-only, 2026-11-19.

Scope: the set v3 (Hawaii + Puerto Rico/USVI, Guam/CNMI, American Samoa) in build_coast_v2.py (SETS, use_set, --set,
per-region speck/tol, keep-alive _get), coast-build.yml set coast-v3, build_alert_coast.py + alert_coast_v3.json,
COAST_SET in app.py / index.html / overlay.js.

Checked sound by the reviewer:
- v2 stays byte-reproducible: its format key is unchanged, NOAA_CREDIT_V2 is the old text, and "set" in index.json is
  outside the tier-1 hash.
- V2_HAWAII_CELLS matches the pinned v2 build cell by cell.
- Cross-set misuse is caught: format_key, and is_real requiring the set's own regions.
- COAST_SET yields whitelisted values only. The page is unchanged when unset. COAST_V2=1 (production) still resolves to v2.
- The workflow is right for all six set values.
- The alert builder has no v2-only assumptions.
- No P1.

| # | Sev | Finding | Fix |
|---|-----|---------|-----|
| 1 | P2 | `_coast_set` only checks that alert_coast_<set>.json is in the checkout. COAST_SET=v3 set before the coast-v3 upload means every visitor's /static/coast/v3/index.json is a 404 (cached 4 h), so the overlay draws unclipped over land. | overlay.js: a coast set whose index.json answers 404 falls back to static/coast/v1, once (CoastStore fallbackUrl). Other failures keep the old behaviour. Asset 2.31.1, playback test. The release order (upload, read-back, then the Render variable) stays the rule. |
| 2 | P3 | The v3 pin was laptop-only. On a runner mismatch no artifact is kept to compare. | Runner dry run #731 reproduced 0881b635 (pin note updated). coast-build.yml keeps a failed coast-v2/v3 build as `<set>-unchecked` (never uploaded). |
| 3 | P3 | `_get` no longer follows redirects (urllib did), a 3xx is retried 4 times, and `_get` has no tests. | A 3xx raises at once (the bucket never redirects). Tests with a fake connection cover keep-alive reuse, a reset (fresh connection), 500 then 200, an immediate 404, a 301, and exhausted retries. |
| 4 | P3 | Nothing ties --upload's prefix to --set (`--set v3 --upload static/coast/v2 --replace` would overwrite the live v2). | main refuses any prefix other than static/coast/<set>; tested both ways. |
| 5 | P3 | The upload-gate test covers set v2 only; `use_set` from main is never undone. | `test_v3_upload_gates`: a synthetic build and a v2 build are refused under --set v3, and set v2 is restored in `finally`. |
| 6 | P3 | `test_coast_set_reads_the_environment` accepted v2 for bad values. | Every bad value must give exactly v1 (blank-padded " v2 " gives v2). |
| 7 | P3 | build()/check() stamped format_key/SOURCE from the active set, not the `regions` argument. | Both use format_key(regions) / source(regions); `test_index_records_the_regions_built`. |
| 8 | P3 | OUT_V2 in build_alert_coast.py is unused. | Removed. |

After the fixes:
- v2 still rebuilds to 9a0ca75f, and v3's local build checks clean at 0881b635.
- Suites: coast 62, alerts + page flag 66, client 1,348.
