# G60: weather at land forecast points (plan section 91)

Branch `feat/land-weather` off production 8ab928e. The record of the test-site verification (step 4), the review (G60) and
its fixes.

## Step 4: test-site verification (2026-11-09)

The test site (`test` @ f41b786, UI 1.30.0; then 543e939, UI 1.30.1) with `LAND_WEATHER=1`. Headless Chrome with its own
profile and the GPU, trusted mouse / touch input. The check scripts are in the session's scratch folder `lw91`.

### Data
- Open-Meteo: at central Oahu, Denver, the Swiss Alps and Alice Springs, every model's rows equal an independent read of
  the same Open-Meteo answer. That covers temperature, dew point, wind, gust, direction, the step precipitation sums and
  the 24-h totals: 0 mismatches.
  - Rows (GFS / ECMWF / ICON / AIFS): 198 / 80 / 107 / 59.
  - AIFS has no gust column.
  - NWS is offered only at the US points.
  - A sea point beside them still answers its swell table.
- NWS: Oahu (68 rows) and Denver (66) equal the api.weather.gov grid, 0 mismatches.
- `/healthz` weather: 6 Open-Meteo calls counted 36 (6 each, as designed), 4 NWS calls, no errors.

### Page
- Desktop 1280x800:
  - a link opens the window minimised and a click expands it;
  - the bar reads GFS | ECMWF | ICON | AIFS | NWS, and each tab loads by a trusted click in 0.4-0.6 s with `model=` in
    the address;
  - Summary shows 16 days;
  - Graph shows three charts on 364 hourly slots;
  - Metric changes every unit;
  - the credit and the total are on the meta line;
  - no sideways page scroll, console clean.
- Phone 375x812: taps on AIFS and NWS work; no sideways scroll.
- The point tool, with real clicks:
  - a land click (central Oahu) opens the weather table in 1.7 s, GFS first, all five tabs; the tool ends;
  - a sea click off Kaena opens its swell table (209 rows, no weather buttons).
- Smoke: 51201 and 46026 GFS tables with their bars, 45007 GLWU and a sea point unchanged.

### The failure path
- A local server of the branch, with Open-Meteo and the NWS on a closed port:
  - every land request answers "Weather temporarily unavailable" (not final, not cached);
  - a sea point beside it still has its swell table;
  - a land click puts the message in the tool bar ("Weather temporarily unavailable · Click another point."), keeps the
    tool on and keeps nothing;
  - a link shows the error box with Retry.
- The test site, with the first answer for a land point swapped for that failure (DevTools Fetch): the error box and
  Retry; Retry loads the weather table (198 rows).

### Found and fixed (UI 1.30.1 @ b4ce6ae, golden aed57f0)
- V4-1, the frozen Time column after a reload with the window open:
  - The window settles narrower once the map's corner controls are measured, which happens after the table is measured.
  - A table narrower than the window shrinks with it, but `--date-w` kept the first value.
  - So the frozen Time column covered 15.6 px of the weather table's Temp column at 1280 px. A swell table at 1920 px
    lost 8.6 px of its first column; that part is older than this section and is on production too.
  - Fix: a ResizeObserver on the table's box measures the frozen columns again.
  - Tests: a test, and a mutant (no observer) that the test catches.
- V4-2, the Location line read "pt_21481N_157986W (21.48N 157.99W, 988 ft)". It now reads as a swell point's, with the
  elevation the temperatures are for: "21.481N 157.986W (elevation 988 ft)".

Suites at aed57f0: pytest 2,757 passed + 1 skipped; Node UI 613, overlay 616.

### Open-Meteo refuses the test server (2026-11-09, after UI 1.30.1 went live)
- From about 18:58 UTC every Open-Meteo request from the test service answered HTTP 429:
  "Daily API request limit exceeded. Please try again tomorrow."
  - The same request from the laptop answered 200.
  - The site's own count that day was about 50 calls (roughly 300 counted, against 10,000), so other services sharing
    Render's outbound address had used up the free tier's daily allowance.
  - Production very likely shares those addresses.
- V4-3, fixed @ d1634b4 (server only; test @ dc6ebe4):
  - the 429 block is checked before the failure memory, so the point that met the 429 reads "busy", not
    "temporarily unavailable";
  - `_capped_get` keeps the first 300 characters of a 429's body (`HTTPStatusError.detail`), so `/healthz` names the
    limit;
  - `block_for`: 1 min by default, 10 min for an hourly limit, 30 min for a daily one.
  Checked live: the daily-limit reason in `last_error`, a 1,799-s block, the answer "Weather is busy".
- OWNER DECISION (2026-11-09): **section 91 paused** before G60, rather than a paid Open-Meteo key, accepting outages, or
  our own GFS / ECMWF data. Nothing reached production (production @ 8ab928e); the test site keeps LAND_WEATHER=1.

### Notes
- The Open-Meteo budget is charged before each call, failed or not: a timeout may still have counted at Open-Meteo. A
  failed point is not asked again for 60 s, and only a click or Retry asks again.
- With the point tool on, a click on a station marker (a wind flag) uses the marker's position (the section-31/39 rule).
- NWS's elevation differs from Open-Meteo's (564 vs 679 ft at central Oahu); NWS's precipitation covers about 3 days.
- ECMWF / AIFS can be on an older run than GFS / ICON; each model's run time is shown.

## Section 91c: the visitor's browser asks Open-Meteo (2026-11-09)

### What changed
The owner chose to have the visitor's browser ask Open-Meteo, because its free limit is per IP and Render's outbound
address is shared. The server no longer calls Open-Meteo's forecast at all.
- `GET /api/forecast` for a land point with an Open-Meteo model answers `om_url` and `om_key` with no rows.
- The page fetches `om_url` itself and POSTs the text to `POST /api/forecast/weather`.
- The server checks the body (place, hours, length, physical bounds, some temperature, a 1.2-MB cap) and builds the
  same table, Summary and graphs. Nothing is cached or logged.
- Commits 2613447 (code, UI 1.30.2) and 79dba4d (golden) on feat/land-weather; test @ 0a5d3f2.
- Suites: full pytest 2,781 passed + 1 skipped; UI Node 625. Mutants: 26 of 27 killed. The survivor is the
  Content-Length check, which the capped read just below backs up, so it behaves the same.

### Test site (UI 1.30.2 served from 20:52 UTC, forecast.js = the commit)
- **The route** (`lw91/v91c_api.py`): for Oahu, Denver, the Alps and Alice Springs:
  - the GET gives `om_url` and no rows;
  - the text fetched from `om_url` on this laptop and POSTed back gives 16 tables (4 points x 4 models);
  - each table matches my own read of that same text: 0 mismatches (values, step sums, 24-h totals, AIFS without
    gusts, each model's own steps);
  - NWS is served as before;
  - every answer carries `Cache-Control: no-store`.
- **Refusals:**
  - an NWS model or a station: 400, final;
  - another place's answer, or garbage: "Weather temporarily unavailable";
  - 1.3 MB: 413;
  - a sea point: its swell table (as designed);
  - `render=full`: "Weather needs JavaScript in this browser".
- `/healthz` weather: `om_built` 16, `om_refused` 2, meta.json read; the server made no Open-Meteo forecast calls.
- **The page's requests** (`v/v91c_net.js`, headless Chrome):
  - one Open-Meteo fetch for the point, reused for the four tabs, Metric and UTC;
  - one POST per new table;
  - none for a station or a sea point;
  - no console problems.
- Desktop 1280x800, phone 375x812, the point tool (land -> weather through the prefetch path, sea -> swell) and the
  smoke run all pass as in step 4.
- **Failure paths** (`v/v91c_fail.js`, DevTools on api.open-meteo.com):
  - a refused connection: "Weather temporarily unavailable" + Retry;
  - a 429: "Weather is busy right now; try again in a moment" + Retry;
  - Retry with Open-Meteo answering: the table.
- **A model tab pressed, then at once another station** (`v/v91c_race.js`, 0 / 150 / 400 ms): the station's table stays
  every time; the late weather answer never lands.
- The test site's storage was emptied and the browser stopped.

### Deviation
- The planned tooltip with Open-Meteo's own reason on a 429 is not built; the message alone is shown.

## G60: two fresh reviewers (2026-11-09)
Reviewer A read the code and checked the data with no browser; reviewer B used the test site in headless Chrome. Both
reviewed feat/land-weather @ ca83ebe (UI 1.30.2). Reports: scratch `g60/a/g60-a-report.md` and `g60/b/g60-b-report.md`.

### Count
- A: 1 P1, 9 P3. B: 1 P1, 6 P3. A-1 and B-1 are the same defect, so **0 P0, 1 P1, 0 P2, 15 P3** in all.

### What held up
- **Values:** 0 mismatches between an independent read of Open-Meteo's text and the built tables.
  - A: the Oahu capture, 4 models, each model's own steps.
  - B: 16 land points worldwide, 60 tables, against the text the page itself fetched (both poles, the date line on
    both sides, Tibet at 4,945 m, Lake Titicaca).
- **GET / POST parity:** the POST builds exactly what a server-side build gives, in every unit and zone tried.
- **No Open-Meteo forecast call from the server** on any path.
- **Refusals:** stations, sea points, sheltered points, oversized bodies, another place's answer, unknown models.
  - 53 hostile bodies: none caused a 500.
  - Nothing is cached; a refused body is never logged.
- **The 0.3-degree place check is safe for real answers:** Open-Meteo's grid point lies at most 0.154 deg from the click,
  at 18 coastal and island points.
- **The loader:** a superseded load cancels its Open-Meteo fetch and never writes a late answer. The held text is reused
  across tabs, units, zones and Retry.
- **The page:** views, units, zones, the point tool on desktop and phone, the frozen columns, a11y facts, console.
- **Stations and sea points:** the same JSON as production.

### Findings and outcomes
| # | Finding | Outcome |
|---|---|---|
| A-1 = B-1 (P1) | NWS tab where api.weather.gov has no grid (southern Canada, northern Mexico, the Bahamas, Chukotka), or NWS pressed in the US then a land point abroad: the GET answered GFS + `om_url`, but the page POSTed `model=NWS` and the route refused it ("Not a land forecast point", final, the bar gone, stuck on reload) | Fixed: the POST carries the model the GET answered (`postQuery`); the route reads the model case-blind |
| A-2 (P3) | The POST route compared the model case-sensitively | Fixed with A-1 |
| A-3 (P3) | `elevation` unchecked: 1e300 printed a 300-digit height; NaN reached the generic error | Fixed: finite, within -500..9000 m, not a bool, else no elevation |
| A-4 (P3) | The record's "byte-identical with the flag off" holds for the API answers, not the page | Wording corrected below |
| A-5 (P3) | An NWS row with no rain block ending in its step still shows a Type (from the weather layer) | Recorded (code-reading only; not seen in the fixture) |
| A-6 (P3) | `nws_type` read "ice_fog" / "freezing_fog" as ice | Fixed: fog, haze, smoke, dust, blowing sand and volcanic ash are skipped |
| A-7 (P3) | The five meta.json reads ran one after another in the request | Fixed: read side by side when more than one is due |
| A-8 (P3) | Two loads of one point at once each ask Open-Meteo | Recorded (one extra call against the visitor's own limit) |
| A-9 (P3) | The step boundary comes from the server's meta run, not the body's | Recorded (a window of minutes around a run) |
| A-10 (P3) | Test gaps: 13 surviving mutants | Fixed: tests for each (cell values for GFS and AIFS, repeated or missing hours, the 6-h meta keep, NWS steps and its 24-h total, the models offered abroad, a sea point asked with a weather model) |
| B-2 (P3) | A transient 502 / 520 on the POST showed the error at once (2 of ~95 POSTs) | Fixed: one quiet retry with the same text on a gateway error (502, 503, 504, 520-524) or a dropped connection |
| B-3 (P3) | The tool ignores a second click while "Checking that point..." (up to 20 s) | Recorded |
| B-4 (P3) | After a 429 the tool said "Click another point." | Fixed: "Click / Tap the same point again in a moment, or another one." for passing failures; final refusals keep the old text |
| B-5 (P3) | Ice and thunder colours below AA on the precipitation band (4.0 and 4.45:1) | Fixed: 5.31 and 5.76:1 |
| B-6 (P3) | The no-script select lists ECMWF / ICON / AIFS, which answer "needs JavaScript" | Recorded |
| B-7 (P3) | A unit or zone change during the first Open-Meteo fetch fetches again (cost 5.83 at Open-Meteo); the author's `v91c_fail.js` sent its station switch to `window`, so that check switched nothing | Recorded (`v91c_race.js` and B's runs cover the case) |

**Correction to the step-4 record (A-4).** With the flag off:
- the API answers are byte-identical to production;
- the page differs in 16 lines (the head variable and its rule, four hidden weather buttons, CSS, the off flags);
- `/healthz` gains a `weather` key;
- `POST /api/forecast/weather` answers 400 where production answers 404.

### The fix round
- **Commits on feat/land-weather:**
  - 9a76315 the fixes (UI 1.30.3);
  - 9ee199b the golden;
  - 741d91c a test of the page's retry flag.
- **test @ c89a7c2** (cherry-picks), UI 1.30.3 served = the repo; the 1.30.2 URL answers 404.
- **Suites:** full pytest 2,833 passed + 1 skipped; Node UI 629.
- **Mutants:** 22 of 22 killed (M13, M24, M25, M26, M28, M36, M38, M40, M46, M52, J19, N1-N11).

### Live check of the fixes (test site, headless Chrome, 2026-11-09 ~22:00 UTC)
- **A-1, no grid:** the NWS tab at Montreal, Tijuana, Hermosillo and Chukotka.
  - The GET asks NWS; the POST asks GFS (200); the GFS table shows (195 rows).
  - The bar stays, with GFS lit; no error; no `model=NWS` left in the address.
  - Fairbanks (a real grid): the NWS table, 65 rows.
- **A-1, second path:** NWS pressed at Fairbanks, then Paris and Montreal from the picker. Each gets its GFS table; the
  POST asks GFS.
- **B-2:** POSTs answered by DevTools.
  - One 502, or one dropped connection: the table, after two POSTs and one Open-Meteo fetch, with nothing shown on the
    way.
  - Two failures: "Weather temporarily unavailable" + Retry. Retry gives the table with no new Open-Meteo fetch.
- **B-4:** after a 429 the tool bar reads "Weather is busy right now; try again in a moment / Click the same point again
  in a moment, or another one." The phone says "Tap". The tool stays on and nothing is kept.
- **B-5:** served colours: ice rgb(0,110,120) 5.31:1, thunder rgb(154,69,8) 5.76:1.
- **Regression:**
  - Oahu, the Alps, Alice Springs and Houston, every tab, rows checked against the page's own Open-Meteo text: 0
    mismatches. The only differences are 0° shown for Open-Meteo's 360°, the same direction.
  - Views, ranges, units and zones at Kiruna: as before, with no extra Open-Meteo fetch.
  - The point tool on desktop and phone: land, sea, a kept point reopening.
  - Console: only the injected errors.
- The test origin's storage was emptied; the browser was stopped and its profile removed.

### Owner awareness
- In southern Canada, northern Mexico, the Bahamas and Chukotka the NWS tab is still offered. Pressing it shows the GFS
  table with GFS lit, because api.weather.gov has no grid there.

## Re-check of the fix round (one fresh reviewer, Fable)

Scope: `git diff ca83ebe..741d91c` and the test site at c89a7c2 (UI 1.30.3). Result: **0 P0, 0 P1, 0 P2, 4 P3**. Every claim
of the fix round held (R1-R9). Of 25 mutants, 19 were killed and 6 were equivalent. No secrets or stray files.

| Id | Finding | Outcome |
|---|---|---|
| F-1 | The type colours passed AA on the plain band but not under the now-row and hover tints | Fixed: rain #11549f, snow #653cac, mix #a41353, ice #005d66, thunder #8b3e07, at least 4.62:1 on the band under every tint; a test reads the colours and tints from the template |
| F-2 | An NWS press answered as GFS was cached under the NWS key only, so the GFS tab asked again | Fixed: `keep()` also files the answer under the model it carries; a failed build is kept under neither |
| F-3 | freezing_spray and ice_crystals read as "Ice"; the skip list held entries that never matched | Fixed: `NWS_NOT_PRECIP`, the dead entries removed |
| F-4 | The quiet second POST went out at once and ignored Retry-After | Fixed: 0.8 s, or Retry-After clamped to 0.8-3 s; a load superseded during the pause sends nothing |

The fix round: 3a50c1b (UI 1.30.4) and the golden in its own commit, 51b02b0. One existing test changed on purpose: the
section-64 SWAN test now expects the GFS answer to be served from the cache (no request). Suites: pytest 2,833 passed + 1
skipped (only the four golden checks failed before the re-capture). Node UI 631 and overlay 616 passed. Mutants of the
fix round: 9 of 9 client mutants killed (no pause, Retry-After ignored, no cap, no floor, abort ignored, the drop
without a pause, no second key, a failed build kept, the wrong second key).

Live on the test site (test @ a6df59d, 1.30.4 served from 22:43:37 UTC, the same bytes as the repo, 1.30.3 answering
404), headless Chrome with trusted clicks:
- Served colours: the five new values.
- A 502 with Retry-After 2: the second POST went out 2,056 ms after the first, and the table drew (194 rows, one
  Open-Meteo fetch).
- A 503 with no header: 823 ms.
- Montreal NWS, answered as GFS: ECMWF needed one GET and one POST (the Open-Meteo text was reused). GFS after that
  needed **0** requests.
- The console showed only the two injected errors. Test-origin storage was emptied and the browser stopped.
