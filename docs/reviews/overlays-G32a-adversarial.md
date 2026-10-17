# G32a — adversarial review of the sea-level pressure field and the geo Worker (plan section 46, step 1)

Target: `feat/isobars-geo` @ 7612ff6 (6d9c842 the job + the Worker + the TEMPORARY `iso-dryrun.yml` /
`iso_dryrun_check.py`; 7612ff6 a test pin), diff against production 00f5d0e. One fresh-context reviewer (Opus 5.5, HIGH),
no R2 write access; NOAA's bucket, data.ecmwf.int and the live frames bucket read-only with the site's verification
User-Agent. Inputs: real GFS PRMSL records (2026101706 f000/f003/f120/f384), real ECMWF msl records (2026101700 steps
0/72/150/360), the live GFS and ECMWF manifests, the live client 2.18.4, the runner dry run (GitHub run 39058137181).

## Summary

**0 P0, 0 P1, 1 P2, 6 P3.** The data are right: every half-grid pressure cell checked equals an independent eccodes
decode within half a quantum (GFS 4 steps, ECMWF 4 steps, all cells, not samples); `PRMSL:mean sea level` is exactly
one record in NOAA's `.idx` (record 1; `MSLET` is record 557) and its f000 "anl" record passes the identity check
(stepRange "0"); the five older fields are byte-identical (the digest pin reproduces on 00f5d0e; a real ECMWF step built
with both commits gives identical PNGs, 6 of 7 also equal the live bucket objects byte for byte, the 7th only by zlib
build); the live client 2.18.4 accepts a manifest with `pres` (13.3 KB) and never offers the unknown field; the rebuild
guard accepts the live GFS 2026101712 and ECMWF 2026101700 manifests for a `force` backfill and still refuses changed
ranges; the Worker's CORS is an exact allow-list with no wildcard and `no-store`, and Cloudflare documents
`request.cf.latitude/longitude` on every plan. The findings are one precision trade-off and test / documentation gaps.

## Findings and outcomes

| # | P | Finding | Outcome |
|---|---|---|---|
| A-1 | P2 | 900-1080 hPa (0.709 hPa a code) moves 2-hPa isobars noticeably: on real GFS PRMSL 2026101706 (half grid, abs(lat) <= 60) 6.4-6.5 % of the true isobar points land more than one 0.5-deg node (55 km) from the line of the quantised field (940-1060: 3.4-3.8 %, 900-1060: 5.2 %, 920-1060: 3.8 %). The large offsets sit in flat highs and cols, where the client's planned half-code-bounded smoothing cannot pull them back. The 1080 top only serves the reduction over the Antarctic plateau (every sea value read was <= 1056). | OWNER DECISION 2026-10-17: **920-1060 hPa** (0.551 hPa a code). A typhoon eye deeper than 920 loses its innermost rings; a land high above 1060 shows no lines inside the patch; both are counted in the run summary. Mutant "back to 900-1080" killed. The step-3 tracer will also be tested on a real quantised PRMSL frame (the reviewer's alternative (b)), not only on a synthetic ramp. |
| A-2 | P3 | `shared_ranges` let the rebuild guard accept a manifest whose `fields` block was empty, truncated, renamed or carried an extra field (production refused all of these): "added since" and "lost" were indistinguishable. | FIXED: `run.comparable_ranges` + `ADDED_FIELDS = ("pres",)`: only a field named there may be absent from an old manifest; anything else about the old block compares unequal (exit 2). Tests: `{}`, a lost `hs`, a renamed `Hs`, an extra field, `hs` alone all give 2. Mutants (any missing field skipped; extra old fields dropped) killed. |
| A-3 | P3 | The clamp report works but nothing pinned it (swapped low/high counts, the line never printed, a wrong `clamped_low` all survived). | FIXED: `run.pressure_summary(frames)` returns the line; a test pins it exactly with real encode stats of a grid holding cells under 920 and over 1060, and pins those stats. Mutants S1, S2, S7 killed. |
| A-4 | P3 | The GFS pressure value pin tolerated a systematic +0.3 hPa (one constant, half-quantum tolerance). | FIXED: a test pins `GfsSource.decode`'s pressure as the PRMSL record / 100 exactly (float32), over a 925-1055 hPa ramp. Mutant S3 killed. |
| A-5 | P3 | The Worker's CORS test missed the `"null"` origin and look-alike hosts (code correct today). | FIXED: the test covers `"null"`, `evilallshoresurf.com`, `allshoresurf.co`, upper case, a port, a trailing slash, a suffix host and a subdomain. Mutants (null allowed, suffix match, case-insensitive, wildcard) killed. |
| A-6 | P3 | The public Worker can use up the account's daily Workers allowance (Free plan: 100k requests a day, shared with the cron trigger Worker), unverified. | DOCUMENTED: `tools/geo/DEPLOYMENT.md` adds the free WAF rate-limiting rule for `geo.allshoresurf.com` (30 requests / 10 s per IP, block 10 s) for the owner to create with the Worker; a blocked page opens as before. |
| A-7 | P3 | DEPLOYMENT.md said to set `GEO_BASE` on Render although nothing reads it before the page step; `test_geo_worker` skipped silently without node on CI. | FIXED: DEPLOYMENT.md sets `GEO_BASE` only once the page code (step 7) is deployed; the test fails on CI (`CI` set) when node is missing. |

Also removed before the merge: the TEMPORARY `.github/workflows/iso-dryrun.yml` and
`tools/model_frames/iso_dryrun_check.py` (a test asserts they are gone).

## Not checked by the reviewer (recorded)

Old-field bytes on real GFS-Wave records (no local JPEG2000 decoder; the ECMWF real step and the synthetic digest pin cover
the code path); a full-length runner build; the account's Workers plan; the Worker on its deployed domain (after the
merge); the client's tracer (steps 3-4); a winter month's land highs.

## Fix verification

Job suite 254 passed (+ the temporary-files test); Worker tests 7 passed; mutants 9/9 (job) + 4/4 (Worker CORS) killed.
Runner dry run on the fix commit f050558 (GitHub run 39063668348, eccodes 2.48, 920-1060 hPa): GFS 2026101712 steps
0/120/384 and ECMWF 2026101712 steps 0/150/360, 779,760 half cells each within 0.276 hPa of eccodes (half a quantum),
0 clamped (GFS 937.1-1058.4 hPa, ECMWF 941.4-1047.5), pressure PNGs 56-59 KB; both jobs' entry points print the
summary's pressure line. Then the temporary files were removed.
