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
