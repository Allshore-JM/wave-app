# Section 73: wind stations through WMO WIS2, the Hong Kong Observatory and SMN Argentina — test site and review (G47)

Branch `feat/wind-wis2` (off production 7f86719). Test site `test` @ f33d067 (step 2).

## Step 1: build (2026-11-01 UTC)

`wind_more.py` gained the WIS2 / HKO / SMN parsers and feeds (`Wis2Provider`, `HkoProvider`, `SmnProvider`) and the WIS2
window branch; `chain_tls.py` completes the incomplete certificate chains of the SAWS and IMD nodes (owner decision
2026-11-01: the CAs' published intermediates, pinned by SHA-256, for those two hosts only); `wind_stations.json` 4,738 ->
4,891 (WIS2 110, HKO 26, SMN 17); UI 1.28.0 (credits per node, subtitles). Tests `tests/test_wind_wis2.py` on real
captures; 35 of 35 mutants killed.

## Step 2: test-site verification

`test` @ f33d067 live 03:58 UTC. On Render every node answered (SAWS and IMD through `chain_tls`), Hong Kong in 7.3 s, SMN
in 3.6 s, no feed error. The site's latest readings against each agency's own answer (independent code): SAWS 27 of 27;
the other nodes 77 equal plus 2 where the site rightly shows no direction (a reported 510 degrees; a calm report with a
direction); Hong Kong 26 of 26; SMN 17 of 17. Windows by trusted clicks (desktop, 9) and taps (phone, 3): subtitles per
node, 24-hour histories (Cape Point 23 rows, Minicoy 8), zones, the per-node credit; no sideways scroll; console clean.
Payloads: stations +15 KB raw / +3.5 KB brotli, latest +4.5 / +0.5 KB.

## Step 3: G47 (one fresh reviewer, 7f86719..ab618f5)

**0 P0, 0 P1, 1 P2, 7 P3.** Every finding was checked by the author before the fix.

Held up: hostile inputs to every parser (directions 0 / 360 / 361 / 510 / negative / NaN, out-of-range speeds, wrong
units, odd times, damaged or oversized zips: each left out or refused, never a server error); the history route (22 rows
from the fixture; 503 with retry on every bad body; one request with the WIGOS filter only; unknown ids 404); the TLS
exception flag for flag against requests' default (expired / wrong-host / self-signed / untrusted-root / incomplete
chains still refused through it); old snapshot rows byte-identical; the page builds the credit from constant tables only;
live values WIS2 104 of 106 equal (the 2 = the designed no-direction cases), Hong Kong 26 of 26, SMN 17 of 17.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| F1 | P2 | WIS2 rows beside their airports' METAR flags (Mangalore = VOML 5.8 km, Thiruvananthapuram = VOTV 3.9 km, Walvis Bay airport placed 79 km off by its reports); near-misses Bhaunagar (1.6 km from VABV) and Agathi (32 km off, at sea); several rows whose node list and reports disagree by more than 5 km | FIXED in the tool (below) |
| F2 | P3 | ~17 WIS2 rows stand by big inland lakes / reservoirs (Volta, Cahora Bassa, Vaal, Hirakud ...), not "two" as the known items said | The rule as decided (lakes of 100 km2 or more count as coast); count recorded here |
| F3 | P3 | `chain_tls`: requests' context carries VERIFY_X509_PARTIAL_CHAIN, so the pinned intermediates were trust anchors themselves (a chain did not have to reach a certifi root) | FIXED: the flag cleared in `chain_tls.context()`; pinned |
| F4 | P3 | The snapshot tool reached SAWS / IMD only through this PC's Windows certificate cache (urllib's default context) | FIXED: the tool reads the two hosts through `chain_tls.context()` |
| F5 | P3 | Hong Kong's Tsing Yi and Tuen Mun left out "for want of a position", but HKO's station list has both | FIXED: both placed (28 stations) |
| F6 | P3 | IMD's node sends the 10-minute and the 1-hour mean for one report; the parser kept whichever came last | FIXED: the shortest averaging period kept (an unstated period counts as the longest; equal periods keep the first) |
| F7 | P3 | Test gaps (mutants): HKO empty answer, `chain_tls`'s proxy path, the zip's declared-size cap, SMN duplicate lines | FIXED: tests for each; SMN keeps the newer of two lines for one station |
| F8 | P3 | Hong Kong timed out for 20 minutes from Render: the 3-s connect / 8-s budget fetch is too tight from Oregon to Hong Kong | FIXED: the far feeds' own fetch (below) |

## Fix round (2026-11-01)

- **F1, the snapshot tool** (`tools/wind/more_stations.py`): a WIS2 station's position is the node's station list where it
  agrees with the reports within 5 km (`WIS2_POS_KM`), else the one checked by hand (`WIS2_POSITION`: Durban, New Kandla,
  Paradip, Ongole, Visakhapatnam airport = the reports; Dwarka, Digha, Pamban, Minicoy, Walvis Bay = the list); a new
  disagreement near the coast leaves the station out with a warning in the build (inland disagreements are left out by
  the coast rule anyway). An airport's own station carries the airport's ICAO id, so its METAR flag stands for it: by
  its WMO number in AWC's station table when the airport is within 30 km (`WIS2_TWIN_KM`: a national id can collide
  with another country's number), or by hand (`WIS2_ICAO`: Bhaunagar = VABV and Agathi = VOAT, which AWC lists without a
  number; CT-AWS, a second station on the Cape Town airfield 1.6 km from FACT's).
  Rebuilt 2026-11-01: `wind_stations.json` 4,891 -> 4,889. Left out now: Bhaunagar, Mangalore, Thiruvananthapuram,
  Walvis Bay airport, Agathi (each for its METAR flag). New: Mormugao (Goa's port, 6.4 km from Goa airport) and Hong
  Kong's Tsing Yi and Tuen Mun. Moved: 52 WIS2 rows, 48 by under 5 km (the list's positions), Dwarka 18 km, Digha 37 km,
  Pamban 9 km, Minicoy 10 km (the hand table). WIS2 106 (IMD 60, SAWS 27, GMet 11, INAM 8), Hong Kong 28, SMN 17. No row
  of another source changed.
- **F3** `chain_tls.context()` clears VERIFY_X509_PARTIAL_CHAIN; both nodes still verify (checked live); `test_swan_cert`
  pins the flags (requests' minus that one) and the proxy path's context.
- **F4** `fetch_stations.get_bytes(..., context=)`; `more_stations.get` passes `chain_tls.context()` for the two hosts.
- **F6** `parse_wis2_features` keeps, per station, time and variable, the shortest `phenomenonTime` period.
- **F8** `app._far_fetch` (connect 8 s, read 15 s, 25 s in all) for the WIS2, Hong Kong and SMN feeds (they run in the
  background scheduler: no request waits on them); `app._far_window_fetch` (8 s, 10 s, 15 s in all) for their windows'
  history (in a request); every other feed keeps `_points_fetch` (3 s, 6 s, 8 s). `wind_more.FAR` names the three.
- Tests: `tests/test_wind_wis2.py` (+7), `tests/test_more_stations.py` (+3), `tests/test_swan_cert.py` (proxy path, flags),
  `tests/test_wind_sources.py::test_app_wires_the_madis_backup` (the far fetch wiring and limits).
- Suites: pytest 2,148 (the G42 trickle test, load-sensitive, alone); fix-round mutants 19 of 19 killed (one test case
  added for the WIGOS id shape after the first pass).

## Test-site check of the fix round (2026-11-01, `test` @ 7cb10bc, live 05:06 UTC)

- Station list 4,889: the five airport stations gone, Tsing Yi, Tuen Mun and Mormugao present; Digha and Minicoy at their
  hand-checked positions.
- Feeds (`/healthz`): WIS2, HKO and SMN published with no error; refreshes 2.3 s (WIS2, two nodes), 2.8 s (HKO), 3.7 s
  (SMN) through the far fetch.
- The site's latest readings against each agency's own answer (independent code): WIS2 100 equal + the 2 designed
  no-direction cases (a 510-degree report; a calm report with 1 degree); Hong Kong 28 of 28 once the site's and HKO's
  times matched (HKO publishes every 10 minutes; for five minutes the site was one step behind, as the feed's 10-minute
  refresh allows); SMN 15 of 15 at matching times (2 newer at SMN).
- Windows: Cape Point 23 rows and Minicoy 8 from their nodes (1.3 s, 0.4 s), Hong Kong and SMN from memory with the
  restart note. No page change in the fix round (UI 1.28.0 as in step 2).
