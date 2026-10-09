# G27 — adversarial review of the tide stations (plan section 38)

Reviewed: `feat/tide-stations` @ 3a987c8 (UI asset 1.18.4; test site `test` @ c3c4438), off production
`Live-Buoy-Update` @ 20839a3 (no tide stations). Two fresh-context reviewers (Fable 5.1, MAX), 2026-10-08, briefed
with the owner's requirements and six decisions, the author's claims and the known items K-1..K-13 (scratch
`g27/brief.md`); reports in scratch `g27/a/g27-a-report.md` (code, data and tests, no browser: 44 harmonic and 53
subordinate stations against NOAA's own answers, a fake-fetch harness of the service, the client's clock maths in
Node, the page block in a harness, 90 mutants, the snapshot against NOAA's live metadata) and `g27/b/g27-b-report.md`
(the test site with trusted mouse / touch / keys in a headless Edge: 10 stations against NOAA's tide tables, desktop
and phone sizes, zones and units, the three windows at seven sizes, errors, load, accessibility; 186 screenshots).
The author verified every finding below (the reproductions re-run, the code read, the snapshot and the thinning
re-computed) before deciding its outcome.

## Result
**0 P0, 2 P1, 5 P2, 21 P3** (A: 1 P1 + 3 P2 + 9 P3; B: 1 P1 + 2 P2 + 12 P3; three of B's P3s overlap A's or are nits
of the brief). The data under a station's name is right everywhere both reviewers looked: served curves byte-equal to
NOAA's at 44 harmonic stations, extremes to the minute at 97 stations, the strip's HIGH / LOW cells equal to NOAA's
tide tables at 10 stations including New York's 25-hour Nov 1, sun and moon to the minute of USNO, no NOAA text as
HTML, no free proxy. The two P1s are a Metric axis-label rounding and the thinning not being monotone in zoom (an
icon shown at zoom 9 can vanish at 9.5); the P2s are two failure paths that cache a degraded day-long answer, NOAA's
own metadata placing four stations in the wrong ocean, keyboard-focusable icons that Enter cannot open, and the parked
chip losing the station's name at tablet widths.

### Reviewer A (code and data)

| # | Sev | Finding | Outcome |
|---|---|---|---|
| A-F1 | P1 | Metric y-axis tick labels are wrong whenever `yScale` picks the 0.25 m step (a 30-day range of 1.40-1.75 m): the ticks 0.25 / 0.75 / 1.25 / 1.75 read "0.3 / 0.8 / 1.3 / 1.8" (`tickText` rounds Metric to one decimal). 10 of the 98 stations fetched that day (Suva, Hunters Point, Unalaska, Nairai, ...); the US branch is right. Author: `tickText(0.25, 'Metric')` = "0.3" reproduced. | FIX: Metric labels with up to two decimals, the trailing zero stripped (the US rule); pin |
| A-F2 | P2 | A subordinate station can be frozen on the cosine fallback for the rest of the UTC day: `_reference_curve` returns None when the reference's key lock is not free within `wait_s` (2 s) or its fetch raises, and `_build` then caches an "ok" cosine until midnight UTC (harness: two subordinates of one cold reference 50 ms apart -> one shaped, one cosine for the day; a transient reference failure -> cosine for the day, still cosine after the reference recovered). Author: the code path confirmed (tide_sources.py 459-511). | FIX: a transient reason (lock timeout, a failed or busy reference) answers "error" (60 s, the page retries) instead of a cached cosine; the cosine stays only for a FINAL reference answer or a missing partner; pins for the three scenarios |
| A-F3 | P2 | Every NOAA 200 error body is "final" for 24 h (+ 1 h in browsers), including throttling-style messages CO-OPS documents without their wording; and an error body on ONE of a harmonic station's two requests is served as a degraded day-long "ok" (hilo [] = no dots / rows, or a cosine curve). Author: `_harmonic` / `_build` confirmed. | FIX: only NOAA's genuine "no predictions" wording is final (the fixture's "No Predictions data was found" and "don't have Predictions"); any other message is a 60 s error; a one-of-two error is an error; pins |
| A-F4 | P2 (P0 by the brief's letter; 4 obscure stations) | Four stations sit in the wrong ocean with the wrong zone because NOAA's own metadata has their longitude's sign (or value) wrong: Niue TPT2891 (+169.9 -> drawn near Vanuatu, zone UTC+11 instead of UTC-11: 22 h off), Neiafu TPT2893 (+6.0 -> off Namibia, UTC+0 instead of +13), San Lorenzo TWC0279 (+78.8 -> the Indian Ocean, UTC+5 instead of -5), Nomuka TPT2897 (+174.8 -> Fiji waters, +12 instead of +13); Djakarta 6835001's latitude (-2.2) puts it 430 km out at sea (zone right). Found by comparing the derived zone with NOAA's `timezonecorr` (16 differ by > 1 h; 12 are NOAA's stale values, the snapshot right). Author: the five snapshot rows confirmed. | FIX: `fetch_stations.py` carries a documented override list for these five positions (the zone follows) and FAILS the build when a derived zone differs from NOAA's `timezonecorr` by more than 3 h outside a listed set of NOAA's known stale values; the snapshot regenerated; a test on the snapshot |
| A-F5 | P3 | A tab open across local midnight(s) restarts the strip from the data in hand and never re-requests, so the last day can lack the curve while its HIGH / LOW rows still list extremes (the curve ends 12 h after the window; Guam after one midnight 4 of 48 samples, after two all; Honolulu after two). | FIX: the midnight restart re-requests the station (revalidating the browser cache) and re-renders (also A-F13) |
| A-F6 | P3 | `clean_name` damages 17 names: acronyms inside mixed-case names ("Martha's Vineyard Gps Buoy", "Cbbt, Chesapeake Channel", "Fort Eustis (Marad)", "Lake Worth Icw", "Grand Bay Nerr", "Uss Lexington" ...) and half-converted apostrophe / dot words ("Port O'CONNOR", "ST.CROIX"). | FIX: only names NOAA writes entirely in capitals are converted; mixed-case names stay as NOAA wrote them; the snapshot regenerated; a test |
| A-F7 | P3 | K-1 is worse than stated: a fourth pair at 0.1 px (Saxis 8573777 / 8633777) and, at zoom 11 where nothing is thinned, 253 pairs closer than 24 px, 140 closer than 18 px (touching), 40 closer than 9 px, 13 closer than 4 px (unclickable). Author: counts reproduced from the snapshot. | FIX: at the last zoom a copy within 9 px of a higher-ranked drawn one is pushed 12 px aside (east, then west, south, north: the first free spot; deterministic by rank and id) so both can be clicked; owner awareness: a few icons sit up to ~1 km off their spot at zoom 11 |
| A-F8 | P3 | America/Havana (24 stations): Cuba's clocks go back 01:00 -> 00:00 on Sun Nov 1 2026, so 00:00-01:00 happens twice; `localMidnightBefore` takes the SECOND 00:00 as Nov 1's start, so Oct 31 gets the 25-hour column and the first hour's extremes land under "Saturday, Oct 31". US zones (2 AM changes) are unaffected. Author: reproduced in Node (Havana hours 24 25 24; New York 24 24 25). | FIX: when the hour repeats at midnight the FIRST 00:00 starts the day; a Havana test |
| A-F9 | P3 | A gap in the curve (NOAA's list of highs and lows incomplete: Christmas Bay 8772132, a 14 h hole) is drawn without a word while the rows list the extremes. | FIX: a meta line names the gap |
| A-F10 | P3 | The drawn path begins / ends with a vertical segment at the strip's edges (the sample 30 min outside the strip is clamped to x = 0 / total: up to ~13 px at Anchorage); and the dots sit at NOAA's exact extremes while the curve is NOAA's 30-min samples (up to 0.10 m apart at Anchorage: a dot 1-2 px above the curve at big-range stations; B saw the now label 0.9 vs NOAA's 0.78 ft there). | FIX: the curve is cut at the strip's edges by interpolation, and the exact extremes join the drawn samples so every dot sits on the curve and the now level follows the real shape near an extreme |
| A-F11 | P3 | A failed station list is re-requested on every rebuild (every pan / zoom end at zoom >= 9) while the server is down. | FIX: 30 s between attempts |
| A-F12 | P3 | `_release_key` can orphan a per-key lock (a waiter that already fetched the lock object keeps an orphan after the entry is deleted): a duplicate build of one key, never a deadlock. | FIX: per-key locks carry a waiter count and are dropped only when it reaches zero |
| A-F13 | P3 | The meta line is not rewritten at the midnight restart ("EDT (EST from Sun, Nov 1)" outlives the change day). | FIX: with A-F5 |
| A-F14 | P3 | Docs and tests: the accuracy sentence (SD 3 / Seattle 4 / Galveston 8 / Nawiliwili 9 cm) holds for the shaped stretches (with the server's own functions 3.1 / 4.6 / 5.0 / 2.4 cm) but the cosine fallbacks inside the same curves reach 15-23 cm and a far or big-range reference (Anchorage <- Nikiski, 10.6 m range) 1.54 m; README says only "no predictions" is final (any error is: A-F3); the Python CI step runs only `tests/coast` (pre-existing scope: `tests/test_tides.py` never runs in CI); the brief said 34 tide tests (30); mutation survivors = test gaps (P16 final TTL, P28/P36 the method word with nothing shaped, P41 a waiter receiving the finished build, J08 the Retry-After cap, J13 an extreme on a midnight / noon boundary, P07 a 21-h gap, P39 a wrong-kind partner within an hour; T08/T09 killed only by the text pin). | FIX: the docs; pins for every gap; CI: OWNER'S CALL (a site-tests job needs the app's requirements installed) |

### Reviewer B (the test site)

| # | Sev | Finding | Outcome |
|---|---|---|---|
| B-P1-1 | P1 | Zooming IN hides icons that were shown: the thinning's kept set is recomputed per rebuild by a greedy pass, so at a finer zoom a station freed from its neighbour suppresses one drawn before. Florida Keys 9 -> 9.5: 13 of 42 icons well inside the view vanish (Wednesday Point, Manatee Creek, Yacht Harbor Cowpens, ...), 9.5 -> 10: 13 of 52, 10 -> 10.5: 11 of 61; South Florida 14 of 59; Charleston 15 of 88; New York 5 of 74; only 10.5 -> 11 loses none. Owner decision 6: "zooming in shows the rest" (the natural gesture loses the wanted icon about a third of the time in the Keys until zoom 11). Author: reproduced from the snapshot (Keys 9 -> 9.5: 12 of 47). | FIX: a REVEAL ZOOM per station, computed once per list (and per opened station), in rank order: the zoom at which the station is 24 px clear of every higher-ranked station revealed before it (z = 9 + log2(24 / d9), capped at 11); drawn when the map's zoom reaches it, so a shown icon never vanishes on zooming in. Author's check on the whole snapshot: monotone by construction, no two drawn icons closer than 24.0 px at any zoom below 11 (2,277 of 3,501 shown at 9, 224 only at 11); the Keys at 9: 62 -> 56 icons, Charleston 114 -> 98, Oahu unchanged |
| B-P2-1 | P2 | Every drawn tide icon is a Tab stop (Leaflet's default `keyboard: true`: 144 stops at Charleston zoom 9) but none can be opened by keyboard: Enter and Space do nothing (Leaflet maps Enter only to a bound popup; the page adds no handler). The live-buoy dots share the gap (3-10 stops, pre-existing); the forecast points are not stops. | FIX: Enter / Space on a focused tide or live-buoy marker fires its click; owner awareness: a busy coast is still a long Tab ride (the forecast points are not reachable by keyboard at all, pre-existing) |
| B-P2-2 | P2 | The parked tide chip loses the station's name at tablet / narrow desktop widths (max-width 40vw - 40 px; the subtitle-gives-way rule exists only in the <= 500 px block): 768 px -> "Tide station · NOAA 16123" and no character of "Honolulu"; 820 -> 1 character; 900 -> "Hono"; 1280 -> 18 of 36 characters of "Sweeper Cove, Kuluk Bay, Adak Island" while the subtitle never shrinks. The live chip has the same defect (pre-existing). | FIX: the chip's subtitle gives way before the name at every width, for both station chips (the phone rule, as the forecast bar does) |
| B-P3-1 | P3 | No cue that icons are hidden between zoom 9 and 10.9 (the legend note is empty, the (i) and tooltips say nothing): a visitor on a busy coast cannot tell the sparse icons are a selection. | FIX: the legend note reads "zoom in for more stations" while any station in view is hidden |
| B-P3-2 | P3 | The observed (dashed) series covers only the elapsed part of today (the strip starts at today's local midnight): 10.6 h at Honolulu at 11 AM, almost nothing just after midnight; the 48-h fetch is mostly unused. A consequence of the redesign. | OWNER'S CALL: accept (the author's recommendation: the owner's example starts at today; the dashed line shows how today's water level tracks the prediction), or give yesterday a column |
| B-P3-3 | P3 | Focus is dropped to the body after the ×, after Escape (focus was on a day button) and after Retry; expanding from the chip focuses the header (good). The live window has the same pattern. | FIX: × and Escape focus the map; Retry's answer focuses the window's header |
| B-P3-4 | P3 | Screen-reader structure: the HIGH / LOW / Sun / Moon row labels are `td` without scope; the moon phase and the sunrise / sunset / moonrise / moonset words are title attributes only; the SVG is "Tide height graph" with no text alternative (the table carries the values: acceptable). | FIX: `th scope="row"` labels, a visually hidden caption, visually hidden words for the sun / moon events and the moon's name |
| B-P3-5 | P3 | Wording: "times in GMT-11 / GMT+12 / GMT+10" and the row labels "(GMT+12)" for Pago Pago / Fiji / Guam (Intl's en-US names; the example site writes SST / FJT / ChST); "(local)" as the HIGH / LOW label when the zone's name changes within the 30 days, with the explanation only in the notes below the fold at 1280x800; the subordinate sentence names its reference by number only ("NOAA station 1611400"); the readout has no zone suffix, so New York's repeated 1-2 AM hour on Nov 1 reads "1:24 AM" for two instants an hour apart. | ACCEPT the GMT±N names (the site-wide rule, the forecast window's labels; owner awareness) and "(local)" (the note explains); FIX: the reference's NAME in the method sentence (the payload gains `ref_name`), the zone's abbreviation in the readout when the zone's name changes inside the strip |
| B-P3-6 | P3 | Phones: a plain tap on the chart leaves the readout showing (the touchend hides it, the browser's synthetic mousemove after the tap re-shows it); it goes away at the next drag's end or a touch elsewhere. | FIX: mouse events within 700 ms of a touch are ignored (the tools' rule), so a tap's readout ends with the tap |
| B-P3-7 | P3 | The callouts of the deepest lows at Anchorage, Pago Pago and Suva have their text box 1-4 px past the chart's bottom edge (the digits whole); Suva's 4.9-ft high sits 3.9 px under the 5-ft top gridline with its callout inside the top pad. | FIX: callouts kept inside the chart (shifted up at the bottom; placed below the dot when there is no room above) |
| B-P3-8 | P3 | At 1280x800 the default tide box covers the forecast bar's ▴ / □ buttons (the bar's title part stays clickable) and the clicked marker, so the active icon shows only once the window is parked; the live window's default box does the same (pre-existing). | FIX: the minimised chips sit above the station windows (z-index 2150), so the forecast bar's buttons stay clickable; ACCEPT the covered marker (the window must open somewhere; its title names the station; owner awareness) |
| B-P3-9 | P3 | The now label and the readout interpolate the 30-min curve linearly: at Anchorage (30-ft range) 0.9 ft when NOAA's 6-min prediction was 0.78 (a chord error up to ~0.12 ft near an extreme); elsewhere within 0.02 ft. | FIX with A-F10 (the exact extremes join the drawn samples); a README word |
| B-P3-10 | P3 | At 1280x800 the default window (66vh = 528 px) shows the chart, HIGH, LOW and Sun; the Moon row and the notes need the window's own scrollbar. | FIX: the default height 66vh -> 70vh (the box-sweep test's order holds: 60 / 70 / 76vh); the rest scrolls, the window is resizable (owner awareness) |
| B-P3-11 | P3 | `tests/test_tides.py` has 30 tests (the brief said 34). | brief nit (A-F14) |
| B-P3-12 | P3 | Brief nit: TWC1867 is Pete Dahl Slough, Alaska, not a Fiji station. | brief nit |

Notes from B, not findings: the 20-stations-in-a-row drill left one strip in the DOM (heap 37 -> 41 MB, old strips
removed); `toggleDay` rebuilds the 30-day table + SVG in 12.8-18.7 ms, a zone change in 8-14 ms, no long task while
scrolling the strip; every text contrast >= 4.6:1 and every graphic >= 3:1; a reload never restores an open tide
window (by design: the chip / geometry are remembered, the station is not).

## Confirmed by the reviewers (the evidence is in their reports)
- A: 44 harmonic stations' served curves byte-equal to NOAA's 30-min curves (0 mismatches, 0 nulls, 1,585 values) and
  extremes to the minute / 0.001 m; 53 subordinates' extremes = NOAA's, 5,893 of 5,926 paired through the published
  offsets and every paired one = reference + offset to the minute, the shaped curve through every extreme to 0.0 m,
  chords <= 5.2 cm, overshoot <= 1.1 cm, gaps only at Christmas Bay; the window inside the strip for every minute x 38
  zone offsets (margin >= 1.0 h); sky chunks = a direct search (|dt| <= 1 s), Prudhoe Bay's polar bands sane, moon names
  and Honolulu's rise / set / twilight = USNO to the minute; hostile ids never reach NOAA, every answer's headers as
  claimed, no HTML sink for NOAA's text, the client list carries only the seven fields; the thinning = an exact O(n²)
  greedy, shuffle-deterministic and translation-invariant (0 pops by construction), 3.7 ms for 3,501 copies; no time
  bombs (the suites pass under four TZs and +400 days); the snapshot = NOAA's live metadata (0 field differences,
  Malakal dropped), zones spot-checked right at 30 places; 90 mutants: 76 killed, 14 survived (4 equivalent, 10 test
  gaps, none a defect by itself). Verdicts: 22 claims, 15 CONFIRMED, 5 PARTLY, 2 not checked (visual).
- B: HIGH / LOW cells at 10 stations + New York's Nov 1 = NOAA's tide tables to the minute and 0.1 ft; the first column
  = the display zone's today, the AM | PM line at the zone's noon everywhere; the now label = NOAA's 6-min prediction
  within 0.02 ft (Anchorage 0.12: A-F10); sun / moon within 1 min of USNO, lit % equal, the glyph mirrored south;
  observed - predicted residuals never a datum offset; console clean in every run; bogus ids 404 no-store; /healthz
  without a tide provider; the gate at 8.9 / 9.0 by wheel and setZoom, no requests below it or when unticked, the credit
  added / removed exactly once; world copies at Fiji and the Aleutians with an animated pan across 180; the thinning's
  spacing >= 24.0 px at 9-10.9, every hidden station covered by a >=-rank neighbour, 0 pops in 1,733 pan comparisons,
  the opened station kept on zoom-out and hidden after close, all drawn at 11, rebuild 3.4 ms median; the window's key,
  no maximise, drag / edges / grip / arrow-key resize with the strip following, geometry restored; the three default
  boxes leave an edge for every pair at 1280x800 / 1366x768 / 1536x864 / 1920x1080 / 1280x650 / 768x1024 / 900x700 and
  each comes forward on a click; chips stacked (tide 8 px above live, the forecast box 13 px above both and below the
  gear, the gear always hit); Escape tide -> live -> forecast; a tool start minimises a window over its bar and a marker
  click during a tool goes to the tool; phones: a 38-px coarse hit area, full screen z 3500, the strip's sideways scroll
  with no page scroll, bars 649 / 700 / 751 with the map above, rotation keeps the station; seq / Abort race both ways,
  503 quiet retries at Retry-After, final without Retry, failed with Retry, observed only with a gauge and silent on
  failure; unit / zone changes with 0 requests and the open days kept; load unchanged at the default zoom (identical
  non-tile requests on / off; tides.js 13.2 KB brotli immutable; first markers 21-30 ms after the list lands). Verdicts:
  40 claims, 36 CONFIRMED, 3 PARTLY, 1 could not check.

## Could not check (both reviewers)
- Subordinate curves against truth (NOAA publishes no curve for them): the method was re-measured on 13 harmonic pairs
  instead (A-F14). NOAA's throttling and the live reference race were not provoked on the shared site (shown with the
  real module and a fake fetch). Real phones and Safari; a real clock change in the display (New York's Nov 1 verified as
  a column built today; the midnight restart simulated by moving `Date.now`). The exposure fan treating the window as an
  obstacle (only the minimise-over-the-bar and click-goes-to-the-tool behaviours measured). The station-list failure
  path on the site (never failed; shown in A's page harness). gunicorn / Linux fork (the fork reset exercised directly).
  Production (untouched).

## Mutation (reviewer A)
90 mutants (42 `tide_sources.py`, 27 `static_ui/tides.js`, 18 template, 4 `static_ui/forecast.js`): 76 killed, 14
survived (4 equivalent: J04, J22, P24, P31; 10 test gaps: J08, J13, J17, P07, P16, P28, P36, P37, P39, P41), 1 not
applied (P40). The table is in `g27/a/g27-a-report.md` section 5; the gaps are pinned in the fix round.

## Fix round scope (step 6; UI 1.18.5, the snapshot regenerated)
Server (`tide_sources.py`, `app.py`, `tools/tides/fetch_stations.py`, `tide_stations.json`):
1. A-F2: a transient reference failure or lock timeout -> "error" (60 s), never a cached cosine; the cosine only for a
   final reference answer or a missing partner; pins (two subordinates of a cold reference, a build in progress, a
   transient failure then recovery).
2. A-F3: only NOAA's "no predictions" wording is final; other messages -> 60 s error; a one-of-two error -> error; pins.
3. A-F4: a position override list (Niue, Neiafu, San Lorenzo, Nomuka, Djakarta) + a `timezonecorr` check (> 3 h fails
   the build outside a listed set of NOAA's stale values); A-F6: `clean_name` only for all-capital names; the snapshot
   regenerated (names and the five positions change, nothing else); a snapshot test for both.
4. A-F12: per-key locks with a waiter count. B-P3-5: `ref_name` in a subordinate's payload.
5. A-F14 / B-P3-9: README (the accuracy sentence: shaped stretches vs fallbacks and far / big-range references; "final"
   wording; the 30-min interpolation; the heading's version; the thinning and reveal rule; K-12); pins for P16, P28 /
   P36, P41, P07, P39 and the template's signature (behavioural).
Client (`static_ui/tides.js`):
6. A-F1 Metric tick labels; A-F5 / A-F13 the midnight restart re-requests (`cache: 'no-cache'`) and re-renders; A-F8
   the first 00:00 when the hour repeats; A-F9 a gap note; A-F10 edge interpolation + the exact extremes in the drawn
   samples; B-P3-3 Retry focuses the header; B-P3-4 `th scope`, caption, hidden words; B-P3-5 the reference's name and
   the zone's abbreviation in the readout on change days; B-P3-6 mouse events ignored within 700 ms of a touch; B-P3-7
   callouts kept inside the chart; pins for J08, J13, J17.
Page (`templates/index.html`, `static_ui/forecast.js` if needed):
7. B-P1-1 the reveal-zoom rule (computed per list and per opened station; `thinTideCopies` becomes a filter on it);
   A-F7 the 12-px push at the last zoom; A-F11 30 s between list attempts; B-P2-1 Enter / Space on tide and live-buoy
   markers; B-P2-2 the chip subtitle gives way at every width (both station chips); B-P3-1 "zoom in for more stations";
   B-P3-3 × / Escape focus the map; B-P3-8 chips above the station windows; B-P3-10 70vh; tests in the page harness;
   the golden in its own commit.
Then: the test site at MAX (the reveal rule by trusted zooming in the Keys, keyboard, chips, the snapshot's names), a
short fresh re-check (MAX), then the owner's production approval (tag `prod-pre-tides` @ 20839a3).
Owner's calls, with the author's recommendations: B-P3-2 accept (today's elapsed observations); B-P3-5 GMT±N zone
names accept (site-wide); B-P3-8 the covered marker accept; A-F14's CI gap: optional, a separate change.

## The fix round (step 6, Opus 5.5 HIGH): UI 1.18.5 @ ae7f40b (+ golden 40da15c)
Every item of the scope above, as planned, with these notes:
- A-F3 as built: only NOAA's "no predictions" wording is final; any other NOAA message, on either of a harmonic
  station's two requests or on a subordinate's extremes, is a one-minute failure. A genuine "no predictions" on ONE of
  the two requests still leaves the other (NOAA's own data incomplete for good: a curve without its list, or a cosine
  through the list), as before.
- A-F2 as built: a reference held by another build -> "busy" (503 retry, not cached); a reference fetch failure, or a
  reference failure remembered under its own key, -> the subordinate's one-minute failure; the cosine only when NOAA's
  data rules the reference out (no such harmonic station, no predictions, no curve).
- A-F4: the regenerated snapshot differs from the reviewed one in exactly 16 names (A-F6), 5 positions and 4 zones
  (Niue -> Pacific/Niue, Neiafu and Nomuka -> Pacific/Tongatapu, San Lorenzo -> America/Guayaquil); 3,501 stations,
  1,260 R / 2,241 S, 238 gauges as before. `KNOWN_ZONE_DIFF` = the 12 stations where NOAA's `timezonecorr` is stale or
  on the other side of the date line (Apia, Kiribati, Tonga, Kanton, Raoul, Easter Island), measured against NOAA's
  live list.
- B-P1-1: the reveal zoom (rank order; a station waits only for a more important neighbour shown before the two are
  24 px apart; d px apart at zoom 9 -> clear from 9 + log2(24 / d); across the date line by the wrapped distance).
  Checked on the whole snapshot before building it: monotone by construction, no drawn pair closer than 24.0 px at
  zooms 9 to 10.99, 2,277 stations shown at 9 (224 only at 11); the Keys at zoom 9 show 56 (greedy 62).
- The owner's calls (2026-10-08, "proceed" after the recommendations): B-P3-2 observed water level for today's elapsed
  part ACCEPTED; GMT+-N zone names ACCEPTED (site-wide); the window over the clicked marker ACCEPTED; the CI site-tests
  job left for a separate change.
Tests: `tests/test_tides.py` 39 (+9: final wording, one-of-two, busy reference, failed reference then shaped, nothing
shaped, waiter gets the build, gap guard and partner kind, the tool's names and zone check, a throttled subordinate and
a remembered reference failure), `tests/ui/tides.test.js` 28 (+9), `tests/ui/tides-page.test.js` 12 (+4), page pins.
Mutation of the fix round (scratch `g27/fix/mut_fix.py`, a copy of the tree): 41 mutants, 39 killed; the 2 survivors
are equivalent (a later reveal keeps both invariants; a nudge below zoom 11 never applies). pytest 867 (the 3 golden
comparisons re-baselined in their own commit), Node 521.
