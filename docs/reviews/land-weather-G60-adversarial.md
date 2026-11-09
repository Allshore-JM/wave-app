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
