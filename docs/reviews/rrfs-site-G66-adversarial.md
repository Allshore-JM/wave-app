# G66: adversarial review of the RRFS site side (plan section 99, steps 6-7)

Reviewed: feat/rrfs 925e6d9..2f3df68 (tables with RRFS_WIND, the land-weather RRFS tab with RRFS_WEATHER, the browser's
Open-Meteo retry and `/api/weather-fail`, the job's `no-store` cell files). Fresh-context reviewer, read-only, a few
small reads of the public bucket. 2026-11-17.

Result: 0 P0, 0 P1, 5 P2, 5 P3. No flag-off leak found: with the flags unset the rows stay 23 cells, the cache keys are
the old tuples, nothing reads RRFS, importing the app loads no numpy and RRFS is never offered. All fixed or recorded;
re-verified the same day (site suite 2,048 passed, every browser test file green, 9 mutants of the fixes all killed,
live in-process check against the bucket: 25 reads, 0 failures).

| # | Finding | Outcome |
|---|---|---|
| P2-1 | The weather tab's 24-h column was blank for ~20 rows: the newest run's f000 (no precipitation) won its hour and the older sources were read for cloud only | FIXED: an hour's missing precipitation and type come from the next older source, like cloud. Live: blank rows 20 -> 14. RECORDED: the first ~14 rows stay blank because no kept source reaches 24 h back (an honest blank, not a partial sum) |
| P2-2 | A stalled job would let day-old RRFS beat a fresh GFS run for up to 3.5 days | FIXED: sources older than 8 h (hourly cycles) / 18 h (runs) past their cycle time are not used |
| P2-3 | A table built while an RRFS read failed was cached as complete (30 min stations, hours for points) | FIXED: the reader marks a merge `degraded`; such a station table is cached for `_WIND_NEG_TTL` only and a point table not at all |
| P2-4 | Every request fetched the pointer when stale (no single flight) and a failed fetch emptied every table's key (a rebuild burst) | FIXED: one refresh at a time, the others served the last good pointer; a failed refresh keeps it up to 1 h |
| P2-5 | The key (newest cycle) missed a run published after a newer hourly cycle | FIXED: the key is the pointer's publish time + the sources in use (`RRFS.version()`) |
| P3-1 | SWAN's alias cache entry lacked the RRFS key | FIXED |
| P3-2 | The summary's gusts could be below its top speed on a handover day | FIXED: gusts only on a day with a gust at every wind hour |
| P3-3 | A bad station slot raised out of `station()` and dropped every source | FIXED: the unpack is inside the per-source try |
| P3-4 | `/api/weather-fail` logged every POST; a superseded load could still report | FIXED: one log line per reason a minute; no report when the load was aborted |
| P3-5 | A thread pool per point read even when every series was held | FIXED: the pool only for the misses |

Open: whether Cloudflare honours `no-store` on the 480 MB cycle files can only be checked once the job change is live
(STOP 2); the current objects still carry `immutable`.
