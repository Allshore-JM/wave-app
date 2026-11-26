# Section 110: live buoys in New Zealand, the Pacific and Chile — test site and review (G75)

Branch `feat/buoys-pacific` (off production 35445e4). Test site `test` @ e436b10 (step 3).

## Step 2: build (2026-11-26 UTC)

`buoy_sources.py` gained four providers: `HilltopWaveProvider` (GW Baring Head South, Tasman Tuatea; sites found from each
council's Hilltop site list; NZST times), `EcanWaveProvider` (Banks Peninsula, the JSON behind ECan's wave page),
`SpcWis2Provider` (the Pacific Community's buoys from the WMO WIS2 global cache's public S3 bucket; BUFR template
301150 + 308015 read in pure Python) and `PuertoAricaProvider` (SIMAR-EPA-01). Priorities 54 (HILLTOP / ECAN / EPA) and
44 (SPC, below CDIP / NDBC / NDBCW). Northland left out (quiet since 2026-08-22; its chain would need `chain_tls.py`, an
owner decision). Tests `tests/test_buoys_pacific.py` on real captures; mutants 17 of 18 (the 18th changed nothing).
pytest 3,131 passed (the bathymetry folder needs scipy: CI).

## Step 3: test-site verification

`test` @ e436b10 live 19:11 UTC: 731 markers, HILLTOP 2, ECAN 1, EPA 1, SPC 1; `/healthz` providers without error
(refreshes 2-12 s). Each window against the agency's own answer at the same moment: GW 2.06 m at 18:36:40Z, Tasman 0.37 m
at 18:55Z, ECan 1.68 m / 6.5 s / 170 deg at 16:20Z, Arica 2.03 m at 19:00Z, Honiara 0.17 m with a 9.3 s peak. Zones
Pacific/Auckland, Pacific/Guadalcanal, America/Santiago. Windows by clicks (desktop: Baring Head, Honiara, Arica) and a
tap (phone: Banks Peninsula); 24-hour histories; the per-agency credit.

## Step 4: G75 (one fresh reviewer, 35445e4..d1c9a9d)

**0 P0, 3 P1, 5 P2, 6 P3.** Every finding was checked by the author before the fix.

Held up: the Hilltop zone (UTC+12 all year, from windows anchored to the servers' own "now"); ECan's epochs (true UTC)
and its direction (FROM; `directionFixed` only turns the page's arrow); Arica's UTC times; the BUFR decoder against
eccodes on all 89 SPC messages then in the cache (0 differences: scales, references, widths, edition-4 offsets,
replication, missing values); the S3 listing (pagination, the cache bounded by the 26-h window, a 404 retried only inside
it); the load (Hilltop 25 requests cold, ~9 every 27 min); thread safety (refresh state touched only inside the
single-flight refresh); the `hilltop:gw:<slug>` ids; the priorities.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | Hilltop answers errors with HTTP 200 and `<Error>`; the site-list and measurement-list parsers read that as an empty list, which was stored for 6 h, hiding the council's buoys | FIXED: `_hilltop_root` raises on `<Error>`; an empty site list while one is held is a failure (held list kept, asked again in 30 min) |
| P1-2 | P1 | GW's Tsig (T1/3, 7.5-8.0 s, near the peak) shown as "Mean Period"; GW's Tz skipped as msecs though it holds seconds (6.0-6.3) | FIXED: Tsig not shown; Tz read as the zero-crossing period (`tz_s`, the window's "Zero-cross Tz"), a value above 100 taken as ms |
| P1-3 | P1 | Honiara: 20 of 86 messages give a 25.6 s peak (the spectrum's lowest band) with directions all round, many from land | FIXED: a peak of 25 s or more is dropped with its direction (`BUFR_LOWEST_BAND_S`); 20.5 s readings kept |
| P2-1 | P2 | A section length past the end raised IndexError (not caught): the refresh failed and the file was asked again every refresh | FIXED: every section length checked inside the message; any decoding exception skips the file, cached as unreadable |
| P2-2 | P2 | A huge section-3 length cost ~2.7 s CPU and ~140 MB | FIXED: section 3 longer than the template's is refused before the loop; subsets 1-64 |
| P2-3 | P2 | One MeasurementList timeout on a cold start lost the council until the next refresh | FIXED: per-site; a failed site keeps its held entry and the list is asked again in 30 min |
| P2-4 | P2 | The DOCTYPE / ENTITY guard was bypassed by a UTF-16 body | FIXED: a body that is not ASCII-compatible (NUL bytes, UTF-16 BOM) is refused |
| P2-5 | P2 | ECan: an absurd epoch (1e20) raised OverflowError and failed the refresh | FIXED: points outside 2001-2099 skipped |
| P3-1 | P3 | SPC direction 0 became None | FIXED: 0 kept (north) |
| P3-2 | P3 | Only the first subset of a multi-subset message was kept | FIXED: every subset read |
| P3-3 | P3 | Tasman's "Sea Temperature" not read | FIXED: in `HILLTOP_FIELDS` |
| P3-4 | P3 | ECan Hmax not rounded | FIXED |
| P3-5 | P3 | README listed the highest wave and mean period for SPC (Honiara sends neither) | FIXED: wording |
| P3-6 | P3 | README said "checked against eccodes on 87 messages"; the committed test checks 3 | FIXED: the README says what was checked once and what the tests check |

Tests added for every finding (`test_g75_*`, 35 tests in the file); mutants on the fixes 9 of 10 (the section-length
mutant is equivalent: an overrun is caught by the next section's check).
