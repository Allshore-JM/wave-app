# G35 — adversarial review of plan section 52 (map polish and the wind feed)

Scope: `feat/map-polish` @ cf49ebb against production b6ac83e (24 files, 10 commits; nothing reviewed before), and the test
site (`test` @ 85a1dd0, UI 1.25.7). Four parts: A city names centred without dots; B a live buoy on a forecast point
(links both ways, bigger touch targets); C forecast points with a blank NOAA bulletin removed; D NOAA MADIS as the backup
of the NWS wind stations, and a stall note. Two fresh-context reviewers at MAX, 2026-10-24:
- **A** (Fable 5.1): code and data, no browser. Suites on cf49ebb: pytest 1,316 passed, Node 994 passed. 79 mutants,
  67 killed.
- **B** (Opus 5.5): the test site and the owner's use, headless Chrome with the GPU and trusted input, at 1280x800,
  1920x1080, iPad 1024x1366, 820x1180 and 1366x1024 with touch, phones 375x812 and 360x640 with touch.

Brief: scratch `g35/brief.md` (the owner's decisions, the author's claims C1-C9, known items K-1..K-14).
Reports: scratch `g35/a/g35-a-report.md`, `g35/b/g35-b-report.md`.

**Result: 0 P0, 0 P1, 3 P2, 18 P3** (distinct; B-1 = A's F2, B-3 = F6, B-9 = F4).

## Findings

| # | Rank | From | Finding | Author's check |
|---|---|---|---|---|
| F1 | P2 | A | The blank-bulletin tool forgets its removals: it scans only the CURRENT list, so its next run records none of the 11, `removed` becomes empty, `BLANK_STATIONS` empty, and an old link to 44062 shows the blank table again (while `--apply` never re-adds anything). README's "comes back only by a re-run with --apply" is wrong both ways. | Confirmed in `tools/build_station_blank.py` `main` (ids from `station_list.json` only; `merge` -> `removed_ids` of the two newest runs). |
| F2 = B-1 | P2 | A, B | Touch screens: Leaflet 1.9.4's canvas `_onClick` takes the LAST drawn dot whose hit area holds the tap, so with the 8-px tolerance a tap ON a dot opens a later neighbour within 11.5 px. Dots whose centre tap opens another: z5 169-172 (production 50-51), z8 21 (13), z10 13 (7), z11+ 7 (7, co-located pairs). Real taps: Oahu z10, 51207's centre opens 51210 (production 51207). | Confirmed from the coordinates and draw order: 46026's centre opens 46247 at z8-10 on touch (7.3 px apart at z10, 46247 drawn later); on desktop 46026 from z9. |
| B-14 | P2 | B | Process: MADIS is ON by default (`WIND_MADIS_URL` unset = the Hawaii query), so the production merge itself would put MADIS data on allshoresurf.com before NOAA is notified (owner decision 4; the plan's step 8 says "once NOAA is notified"). | Confirmed: `app.py` `WIND_MADIS_URL = os.environ.get("WIND_MADIS_URL", wind_sources.MADIS_HI_URL)`; the test site runs it with no env. |
| F3 | P3 | A | `_madis_fetch` has no overall deadline: a server trickling one byte every 2 s held it for 40 s until the server quit (the (10, 45) timeout is per read): one of the wind runner's two workers held. | Agreed (the author's K-4). |
| F4 = B-9 | P3 | A, B | The stall note's time is in the visitor's zone and names none ("4:15 PM" in New York, "10:15 AM" in Honolulu for the same moment). | Agreed (the author's K-5). |
| F5 | P3 | A | MADIS's viewer sometimes answers with every wind field -99999 (two of three samples by curl; the site's own client got full answers); the code keeps the last readings, but nothing shows it: no `last_error`, no MADIS `newest` / readings in `/healthz`. | Plausible; cause not found (3 samples). |
| F6 = B-3 | P3 | A, B | Round trips can switch stations where two stations share a spot (3 live buoys, 6 forecast points): 44088 -> forecast 44014 -> "Live buoy" opens 44014; 46247 -> live 46026 -> "Forecast point 46026 · 1.0 mi away". Correct under the stated rule. | Agreed. |
| F7 | P3 | A | Far-link text at its edges: twins at 1.0-1.6 km read "· 1.0 km away" / "0.6 mi away"; 9.95-9.99 prints "10.0 mi". | Agreed. |
| F8 | P3 | A | Stale text: `get_wind_providers` docstring (no MADIS), README "4-15 s" (measured 10.7-39 s), `MADIS_VARS` unused, plus the author's K-6 (`WIND_STALL_NOTE_S` unused). | Agreed. |
| F9 | P3 | A | Test gaps (surviving mutants, the code right in each): the newest-reading tracker (D26), the link sync after each list landing (B25, B26), the provider's future cut (D27), a busy NWS budget swallowed into a MADIS answer (D15), the pad's neighbour loop and world copies (B19, B23). | Agreed. |
| B-2 | P3 | B | After a link, Escape closes the window BEHIND (the fixed order wind, tide, live, forecast), not the one in front. | Agreed (`static_ui/forecast.js` Escape order). |
| B-4 | P3 | B | The same station under two ids gets no link unless within 1 km: cdip:163 (Majuro, NDBC 52201, 4.3 km), Marine Institute M2 (62091, 1.66 km) and M6 (62095, 3.06 km). | Agreed. |
| B-5 | P3 | B | The link buttons are 31 px tall on phones (the star and shaka beside them 44 px); the far text wraps a 360-px toolbar from 96 to 135 px. | Agreed. |
| B-6 | P3 | B | A screen reader reads the far link's "·" and "mi" literally, and its description repeats the name. | Agreed. |
| B-7 | P3 | B | The touch rules key on the PRIMARY pointer (`(pointer: coarse)`), read once at load: an iPad with a keyboard case or trackpad, or a touch laptop, may get exact taps again. Untestable here: the owner's iPad. | Agreed; to check on the owner's iPad. |
| B-8 | P3 | B | A removed station kept as a visitor's default opens as "Select a station" over the sheltered note in 3 of 4 runs (a race with the station list); a removed favourite vanishes with no note; a removed station's note offers no "Live buoy" although ndbc:46088 reports. | Agreed. |
| B-10 | P3 | B | One fresh MADIS row anywhere hides the stall note while all 31 Oahu NWS flags are grey. | Agreed (`windStallNote` uses `some`). |
| B-11 | P3 | B | A map tool's tap within 11.5 px of a forecast dot lands ON the dot on touch screens (Measure distance 9 px away: test on the dot, production at the tap). | Confirmed in Leaflet 1.9.4 `_fireDOMEvent`: a circle marker of radius <= 10 gives the event its own position. |
| B-12 | P3 | B | Zoomed in close (41113 at z13 on the iPad), the twin forecast dot, drawn visibly apart, still opens the live window: the twin is left out of the live pad's limit at every zoom. | Agreed (`liveTargets` twin exclusion). |
| B-13 | P3 | B | The "nws+madis" source line names the network list twice (230 characters). | Agreed. |
| B-15 | P3 | B | The stall note cannot explain an outage already running when the server starts (`newest` lives in memory): production's merge is that case (0 NWS rows of 294, no note). | Agreed (by design until now). |

## Confirmed held
- **Names**: an independent reference matched `thin` on 6,000 random sets and 19 real views (A); 2,544 names over 1,088
  pan steps at all seven sizes and at a 16-px minimum font: every name centred on its decoded place, no dot, none cut,
  none overlapping, none under a window, panel or tool, none taking a press (B).
- **Links**: `linkFor` reproduced independently for all 563 live buoys (141 same-id, 28 nearest, 394 none; no missed or
  false twin; the list-identity cache is sound) (A); both directions at all seven sizes, the opened window in front, every
  way of reaching the forecast window, the keyboard path, far texts in both units, failure paths (refused or late lists)
  (B).
- **Touch**: Leaflet read; only the forecast dots are interactive paths on the map's renderer; `liveDotAt` = the icon's
  anchor; a pad never reaches another icon's centre (A). Desktop: 656 trusted clicks give production's result. Touch:
  Oahu z10 phone, a tap that opens nothing 1 in 325 (production 84); Florida z9 iPad 7 in 225 (production 135) (B).
- **Blank points**: an independent parser over the 12Z run archive agrees on all 734 old stations (blank = the 11 +
  Alaska_NS3); the server branch, `model=SWAN`, `render=full`, `forgetRetiredDefault`, the IP-nearest list (A); the 11 old
  links on the site, Alaska_NS3 and its neighbours kept (B).
- **MADIS**: units and QC letters from MADIS's own documentation; an independent parser = `parse_madis_csv`; the merged
  table = production's on 400 random lists without MADIS, an independent merge on 600 with it; the fork reset covers the
  provider; the history fallback's rules (A); every island's NWS stations fed (Oahu 66/66, Big Island 102/110), the five
  NWS kinds' windows (B).
- **Stall note**: its boundaries exact (A, B).
- **Standing rules**: 1920x1080 shark test 8.0 / 7.9 fps vs production 8.0 / 8.0, 0 unpainted; North Sea phone 8.0 vs 8.0;
  memory within production's range (B). Console of a fresh target after a full round: 0 errors (B).
- K-3 measured: the densest phone view's full rebuild 8.6-8.9 ms vs 3.3-3.8 ms on production, no long task (B).

## Observations outside section 52 (not counted)
- The head's early `/stations.json` fetch has no catch and no retry: a refused list leaves an unhandled rejection and no
  forecast dots for the session (B).
- NOAA's AWS copy no longer holds per-station bulletins (from the 2026-10-22 18Z run): plan section 53 (author, K-10).

## The owner's decisions (2026-10-24)
1. MADIS (B-14): **keep it on by default**; the owner sends NOAA's notification (draft prepared by the author: to
   `NCEP.List.MADIS_Support@noaa.gov`, the address NOAA's restrictions page links for "To notify NOAA, contact the MADIS
   team"). The production merge waits for the owner's word that it went out.
2. The stall note (B-10, B-15, F4 = B-9): **shown when most NWS flags are grey**: fewer than half of the NWS stations have
   a fresh reading from any source; the time zone named; it works after a server restart.
3. Links between twins (F6 = B-3, B-4): **back to where you came from**: a window opened by a link links back to the
   station it came from; a small table links the same buoy listed under two ids.
4. Far links (K-1): **the map moves there**: a link to a station farther than about 1 km recentres the map on it.

NOAA's restrictions page (madis.ncep.noaa.gov/madis_restrictions.shtml, read 2026-10-24) also asks pages displaying mesonet
observations to "identify the source of the data and provide a disclaimer regarding the accuracy and reliability" (its
example: "NOAA and its data providers disclaim liability of any kind whatsoever, ..."): added to the fix round.

## Fix round scope (step 7; UI 1.25.8)
Server and tool (7a):
1. F1: the blank tool scans the listed ids AND the removed ones; a removed station stays removed while blank in the two
   newest runs; one with data again in both comes back with `--apply` at its place (the order of `station_coords.json`,
   which equals the list's original order) and a printed notice; a test that every removed id was scanned in the two
   newest runs; README corrected.
2. F3: `_madis_fetch` gets a wall-clock deadline (the `_points_fetch` socket-shutdown pattern, 90 s).
3. F5: `MadisProvider` keeps `_newest_t` and a readings count (`/healthz`, `newest.madis`) and logs a warning when an answer
   has data lines but no wind.
4. B-13: the "nws+madis" source line names the networks once.
5. NOAA's disclaimer in the window of every NWS-network station (mesonet observations, whichever feed brought them).
6. B-8 (server half): a removed station's answer carries its coordinates (so the page can offer its live buoy).
7. F8: the docstring, README timings, `MADIS_VARS`, `WIND_STALL_NOTE_S`.
8. F9 (server half): D26 (two stations at different times), D27, D15.
Page and client (7b):
9. F2 = B-1: the map's canvas renderer picks the NEAREST interactive path holding the tap (ties: the later drawn), for
   clicks and hover.
10. B-7: `(any-pointer: coarse)` for the tolerance, the pads and the CSS; the renderer follows the query's change. (The
    owner checks `matchMedia('(pointer: coarse)').matches` on the iPad with and without its keyboard.)
11. B-11: with a map tool active, the tools take the tap's own position (`map.mouseEventToLatLng(e.originalEvent)`).
12. B-12: a live dot's twin is left out of its pad's neighbours only while the twin lies under the dot itself.
13. K-3: one `liveTargets()` pass per rebuild.
14. Decision 3: back links remember where the visitor came from; the alias table (CDIP 163 -> 52201, Marine Institute
    M2..M6 -> 62091..62095, and any other id pair the lists show).
15. Decision 4: a link to a station farther than LINK_KM recentres the map on it.
16. F7: the plain words up to ~2 km (the same spot to a visitor); the distance rounded before the 10 test.
17. B-5: the link buttons 44 px tall under coarse pointers; a shorter far text on narrow screens.
18. B-6: an `aria-label` in words; a title that adds the station's name.
19. B-2: Escape acts on the window holding the focus, else the one in front, then the fixed order.
20. B-8 (page half): a stored default that is no longer listed opens like no default (near the visitor) with its note; a
    removed favourite is dropped with a note; the sheltered note offers the live buoy of the same id.
21. Decision 2: the stall note's new rule and wording (zone named).
22. K-2: a window with a single reading draws it as a point.
23. F9 (page half): B25 / B26 (the link sync after each list landing), B19 / B23 (the pad's neighbours, world copies).
24. Outside section 52, cheap: the head's early `/stations.json` fetch gets a catch and one retry.
Then the test site (MAX) and a short fresh re-check (the fixes touch the hit-test and link code), then production after the
owner's word on the NOAA notification.

## Fix round, server and tool (step 7a, d99e343)
Items 1-8 of the scope, Opus 5.5 HIGH:
1. F1: `tools/build_station_blank.py` scans the listed ids and the removed ones (`scan_ids`); each run records `back`
   (removed ids read with data: not blank, absent or failed); `removed_ids(runs, before)` = blank in both of the two newest
   runs, plus the ids removed before that are not back in both; `--apply` restores a comeback at its place in
   `station_coords.json`'s order (the list keeps that order: pinned) and names it. The committed file's rule text updated.
2. F3: `_madis_fetch` = `_capped_get(url, cap, headers, (10, 45), 90 s)`, the points fetch's socket-shutdown cut, shared.
3. F5: `MadisProvider` keeps `_newest_t` (so `newest.madis` reaches the page) and `status()` gains `readings`,
   `last_rows`, `last_wind`, `newest_age_s`; `parse_madis_csv(stats=)` counts records and wind readings; an answer with
   records and no wind logs a warning (the kept readings stay).
4. B-13: `ATTRIBUTION["nws+madis"]` names the networks once.
5. NOAA's accuracy disclaimer (`MESONET_DISCLAIMER`, the restrictions page's wording) in the history answer of every NWS
   station (`disclaimer`), whichever feed; the page shows it in 7b.
6. B-8 (server half): the removed-station answer carries `lat` / `lon`.
7. F8: docstrings, "4-40 s", `MADIS_VARS` and `WIND_STALL_NOTE_S` removed.
8. F9 (server half): D26 (two stations at different times, both orders), D27 (a report 601 s ahead dropped by the
   provider), D15 (a used-up NWS budget answers busy with a MADIS row present).
pytest 1,320 (full run, goldens included); mutants (scratch `polish/mut7a.py`, a scratch copy) 26 of 26 killed.

## Fix round, page and client (step 7b, 943f6f9 + golden 10988b8 + 685f7c2; UI 1.25.8)
Items 9-24 of the scope, Opus 5.5 HIGH:
9. F2 = B-1: `NearestCanvas` (the map's renderer): a click and the hover go to the NEAREST interactive path holding the point
   (ties: the later drawn; a dragged path is passed over on a click; a path without a point wins only alone).
10. B-7: every touch rule on `(any-pointer: coarse)` (the renderer's reach, the live / tide / wind / My points pads, the
    link buttons); the query's `change` resets the reach and redraws the markers.
11. B-11: `tools.js` takes `map.mouseEventToLatLng(e.originalEvent)` (the event's latlng when that is not usable).
12. B-12: the twin forecast dot is left out of the live pad only within the dot's radius + 3 px.
13. K-3: `requestLiveTargets()` runs one pass after the round of rebuilds (a microtask).
14. Decision 3: `fwBack` / `lwBack` (a window opened by the other's link links back while it shows that station); the alias
    table `LINK_ALIAS` (CDIP 163, MI M2..M6) + the CMEMS `6200NNN` rule.
15. Decision 4: `linkMove` pans to a station farther than 1 km, on the world copy in view.
16. F7: the plain words to 2 km (`LINK_PLAIN_KM`); `linkAway` rounds before the 10 test.
17. B-5: 44-px link buttons on touch screens; at <= 400 px "Forecast point · 19 mi".
18. B-6: `aria-label` in words for a far link; the title = the station's name (else none).
19. B-2: Escape: the window holding the focus, else the one in front (the forecast window only with a station window open),
    then wind, tide, live.
20. B-8: the head forgets a stored default in `retired_stations` (the server's 11) and the page says so once
    (`tellOnPage`); favorites no longer offered are dropped with a note (a list under 100 decides nothing); the forecast
    event carries the answer's lat / lon, so a removed station's sheltered note offers its live buoy.
21. Decision 2: the stall note when fewer than half of the NWS stations are fresh and the feed's own newest is over 3 h old
    (unknown after a restart: "NWS feed: no recent readings"), the time in the stations' zone, named.
22. K-2: `lonePoints` draws a reading that stands alone as a dot (speed, gust, air, water); the disclaimer under the source.
23. F9 page half: pins for B25 / B26, behaviour tests for B19 / B23.
24. The forecast station list: a refused answer asked for once more after 4 s.
Suites: pytest 1,320, Node 1,011 + the new ones (nearest 4, buoylinks 13, window +1, winds +1, tools-ui +1, mypoints +1,
early +1, firstview +1); the rendered page's six inline scripts parse. Mutants (scratch `polish/mut7b.py`): 43, 41 killed at
first, the 2 survivors (the twin test was beyond 1 km; the test machine's own zone is Hawaii) pinned @ 685f7c2: 43 of 43.

## Test-site verification of the G35 fixes (step 7, UI 1.25.8)
`test` @ 01ac49e (cherry-picks of 7a and 7b: d99e343, 943f6f9, 10988b8, 685f7c2; README kept as test's), pushed 02:15:59 UTC
2026-10-25, UI 1.25.8 served from 02:17:09 (12 polls, all 200). Served UI assets = the commit, immutable; the 1.25.7 URL
404; the page carries `RETIRED`, `NearestCanvas` and ten `(any-pointer: coarse)` rules. The test branch's suites: Node
1,010 passed; pytest 971 passed + its 5 known failures (four overlay goldens against test's logo page, the numpy import).
Opus 5.5 MAX. Headless Chrome 155 with the GPU, its own profile, never signed in; trusted mouse, touch and keys (scripts in
scratch `polish/v7`); both origins' storage emptied afterwards, the browser stopped.

### Server and tool
- Answers before (02:13 UTC, the old code) and after (02:27, the new code) on the same runs (GFS 20261024 18 UTC, GLWU 01Z,
  SWAN 23Z): the 13 forecast answers identical; the removed stations' answers now carry their position (44062 38.56 /
  -76.41, 46088 48.33 / -123.16, 44039 41.14 / -72.66); every NWS station's history answer carries NOAA's mesonet
  disclaimer; /healthz MADIS readings 273, last_rows 313, last_wind 293, newest_age_s 1,100, no error; /api/wind/latest
  `newest.madis` present, `fed_by` madis 273 (NDBC relays 0 after the restart: every gauge had a fresh CO-OPS reading).
- The blank tool on a scratch copy against run 2026102418: 734 bulletins (723 listed + the 11 removed) from NOAA's AWS
  tar: blank 11, ice 1 (Alaska_NS3), absent 0, failed 0, back 0; "removed" the same 11; the list unchanged.
- The CMEMS rule over the live lists: 33 CMEMS buoys numbered 6200NNN, 17 with a forecast point 62NNN 0.0-17.8 km away.

### The page (desktop 1280x800, phones 360x640 / 375x812, iPads 820x1180 / 1024x1366)
- B-7: under phone and iPad emulation `(any-pointer: coarse)` true, the reach 8, the nearest-path renderer, live pads up
  to 8 px, the link buttons 44 px. DevTools cannot emulate `any-pointer`, so the query's change listener was not driven,
  and this laptop's own touch screen keeps the touch rules on at desktop size too (V7-1).
- B-1 (`v2_shadow.js`): every forecast dot that a later-drawn dot's reach covers, in eight views (Kaneohe z10, Oahu z9,
  San Francisco z9 / z10, Hawaii z6, the northeast z7, Southern California z8, the Gulf z6), tapped at its centre, 3 px
  away from and 2 px toward that dot: 57 taps; 14 fell on a live dot, tide icon or wind flag drawn over the spot and
  opened that window (the owner's rule: the live dot wins its spot, the forecast is one tap away through its link); every
  canvas tap opened the dot nearest to it but for ties within 0.75 px (re-run of Hawaii z6 and the Gulf z6 with the
  distances logged: 31 of 35 nearest, the 4 others ties: three at 42363 / 42394, two stations 0.0 px apart, and one
  between 51207 and 51210, 1 px apart). Production (no reach): 51203 and 51207 at Hawaii z6 open 51213 / 51210 from their
  own centres; on test they open themselves.
- B-11 (`b16_tools.js`, phone): Measure distance, one tap 4 / 9 / 14 px beside forecast dot 13130: the vertex at the tap,
  as on production (1.25.7 put it on the dot).
- B-12 (`b5_taps.js`, iPad z13 at 41113, the live dot and its twin 11.5 px apart): the live pad 0 there, the forecast dot
  opens from its centre and up to 9 px around (production: its centre only, nothing from 6 px).
- Desktop clicks on this touch laptop (`b5_taps.js`, Oahu z10, 528 clicks 0-11 px from 16 icons): test opened an icon at
  every press (the touch reach, V7-1), production nothing from about 4 px; no click opened an icon farther than the
  nearest except the designed cases (a live dot over its twin; a tide icon and a wind flag at one gauge).
- Links (`b3_reach.js`, trusted clicks): far links (decision 4) 41002 "Forecast point 41002 · 213 mi away" (Metric "343 km
  away"; the buoy has moved since step 5's 203 mi), 41046 168 mi, 42058 90 mi, 51001 73 mi, 46085 71 mi: the map moves
  onto the forecast point (41002: 35.35 / -74.71 -> 32.32 / -75.36) and the forecast window links back ("Live buoy 41002
  · 213 mi away", the station's name as the title); plain words to 2 km with the map moved beyond 1 km (41009 1.6 km,
  46026 1.7 km); the alias table: CDIP 163 -> "Forecast point 52201 · 2.7 mi away", the Marine Institute's M2 -> "Forecast
  for this point" (62091); round trips (decision 3): 44088 -> 44014 -> back to 44088 (step 5: 44014's own buoy), K13a ->
  K13 -> K13a, 46026 -> 46026 -> 46026; no button for aodn:Wooli, Draugen, 44085, 41120, Tantabiddi.
- Link buttons (`v15_layout.js`): 44 px tall; at 360 / 375 px "Live buoy · 213 mi" / "Forecast point · 213 mi", wider
  "Live buoy 41002 · 213 mi away"; a screen reader hears "Live buoy 41002, 213 miles away" (the plain near words have no
  aria-label); the title is the station's name; no sideways scroll of the page.
- Escape (`v2e_escape.js`, `v2f_escape.js`): right after "Forecast for this point" by click, tap and key, and after "Live
  buoy" by key; not after "Live buoy" by click or tap (V7-2).
- Removed stations (`b13b_default.js`, `b13_blank_ui.js`, `v13c_46088.js`, desktop and phone): a stored default 44062 or
  46088 -> the site opens near the visitor (51201), `?station=51201`, the default removed, the note written (V7-3);
  favorites [44062, 41113] -> [41113]. An old link `?station=44062` (plain, `&model=SWAN`, `&render=full`): the sheltered
  note, no Retry, no table, the id as an unknown option, the star and the shaka disabled, no dot. Alaska_NS3 listed with
  its table. 46088's note offers "Live buoy": the live window opens 46088 and the map moves there (1.4 km off the forecast
  point), and its "Forecast for this point" goes back to 46088's note; 44062 (no live buoy with waves) has no button.
- Wind windows during the NWS API stall (`v14_windwin.js`): NWS flags with a MADIS reading Oahu 28 / 29, Maui 36 / 40, the
  Big Island 49 / 58, Kauai 13 / 13 (ages 16-82 min); one window of each kind (utility 016HE, mesonet 011HI, RAWS OFRH1,
  CWOP F0803 calm): the single reading drawn as dots (speed, the gust where reported, air temperature), the current
  reading and the weather line, the "latest report from NOAA MADIS" note, one source line, NOAA's disclaimer line. A
  rewritten "nws+madis" history (`v14b_mixed.js`, six openings, US and Metric): filled in 1.6 s, "The latest reading from
  NOAA MADIS (the NWS API has none newer).", both sources named (one empty read in the first run did not come back).
- The stall note (`v9_stall.js`, `v9b_race.js`, /api/wind/latest rewritten by DevTools Fetch): with the station list
  first, every case as designed: the real answer (273 of 294 NWS stations fresh through MADIS) none; the feed 4 h quiet
  with no NWS rows "NWS feed stalled since Oct 24, 12:52 PM HST" (in HST also with the computer in New York or London);
  the real MADIS rows none; exactly half fresh (147 / 294) none, one under (146) the note; 3 h - 60 s none, 3 h + 60 s the
  note; every NWS row stale_s + 60 s old the note, - 60 s none; a restart (no newest) or a non-numeric newest "NWS feed:
  no recent readings"; a newest in the future or no `now` none; NWS partial: the feed's own "loading more…"; below the
  gate and with the layer off nothing. With the readings first, no note (V7-4).
- The station list asked again (`v12_stations.js`): a failed (network) or 503 first request, the second 4.2-4.4 s later
  and the 795 stations drawn; both failing: two requests, no third, no dots, the select keeps the server's options.
- K-3 (`v17_rebuild.js`, two rounds, test against production, the deferred pad pass included): one pass per rebuild (step
  5: three); the North Sea z6 phone (157 live dots) 5.1 ms against 4.1 (step 5: 9.2), the other views equal; real drags:
  no long task on either site.
- A fresh page target through a full round on the desktop and the phone (`b12_console.js`): 0 console problems.

### Findings
- V7-1 (owner decision): this laptop has a touch screen (navigator.maxTouchPoints 10, `any-pointer: coarse` true,
  `pointer: coarse` false), as many Windows laptops do, so with a MOUSE the touch rules apply: forecast dots open from
  ~11 px (production 3), the live / tide / wind / My points pads, 44-px link buttons. Computers without a touch screen are
  unchanged. OWNER (2026-10-25, AskUserQuestion): keep the larger targets; correct the README and the two template
  comments that say "a mouse hits as before".
- V7-2 (P3): after a click or tap on the forecast window's "Live buoy" the focus stays on that button (Chrome focuses a
  pressed button), in the window now BEHIND the live window, so Escape minimises the forecast window and leaves the live
  window in front (G35 B-2 half fixed; the keyboard path is right). Fix: the link moves the focus into the live window's
  header on every activation, as the forecast window's expand() does for its own link.
- V7-3 (P3): the window's one-time notes (#fwNote: a removed default cleared, removed favorites dropped, and section 31's
  "this point is not kept") are the first child of the scrolling body, and the now-row reveal (section 35) scrolls them out
  of sight (229 px on the desktop, 271 px on the phone; seen only after scrolling the table to its top); and two notes of
  one load replace each other (a removed default and a removed favorite: only the default is told). Fix: the note line
  outside the scrolling body (between the toolbar and the body); the notes of one station kept together.
- V7-4 (P3): the stall note is worked out only when the readings arrive; when they arrive before the wind station list
  (a first visit: the list is ~230 KB, the readings ~100 KB) it is worked out against an empty list and stays empty until
  the next readings, 5 minutes later. Fix: worked out again when the station list lands.
- Owner awareness: six spots hold two or three forecast stations at exactly the same position (41036 / 41064 / 41159,
  41111 / 41115, 46079 / 46105, 46094 / 46097, 63110 / 63111, AGULHAS_FA / FAPLATFORM): a tap there opens one of them
  (the later drawn, as on production); the others stay reachable through the station picker.

### Fix round 7c scope (UI 1.25.9)
1. V7-2: the "Live buoy" link focuses the live window's header on every activation; a pointer-click test.
2. V7-3: `#fwNote` between the toolbar and the body (CSS: margins; hidden when minimised; printed as before); `note()` keeps
   a second message for the same station (no duplicate when a message is told again); window / page tests; the golden.
3. V7-4: the last readings kept; the stall note worked out again when the wind station list lands; a page test with the
   readings before the list.
4. V7-1: README and the two template comments.
5. Pins, mutants, README, the record; then the test-site check (MAX) and the short fresh re-check (MAX).

## Fix round 7c (UI 1.25.9, 2026-10-25)
Code @ b44ba4b, golden @ 75b1ab6 (feat/map-polish, pushed; not on test yet: test is still 01ac49e = UI 1.25.8).
- V7-2: both links call `linkFocus(header)` on every activation (mouse, touch or key), so the focus moves into the window
  the link opened and Escape acts on that window. Test: a pointer click focuses the opened window's header.
- V7-3: `#fwNote` now sits between the toolbar and the scrolling body (`.forecast-win > #fwNote`, `white-space:
  pre-line`, hidden when minimised), so the scroll to the current hour cannot hide it. `note()` keeps the notes of the
  station on screen together, one per line, each once. Another station's forecast clears them. The page's notes (no
  station of their own: a removed default, removed favorites) wait for the first forecast and are then shown on it. A note
  told before forecast.js's `init` is kept for it. `tellOnPage(msg)` tells the module at once when it is there, and
  otherwise once when the window comes up.
- V7-4: the last wind readings are kept (`windLastFeed`), and `workOutWindStall()` runs again when the station list
  lands. A list that lands after the readings now shows the note at once.
- V7-1: the README touch paragraph and the two template comments now say a mouse on a touch-screen computer gets the
  touch reach (owner: kept).
- Tests:
  - window.test.js: the notes test (wait, order, once, before init, another station);
  - firstview.test.js: notes with no station; a listener only without the app;
  - winds-page.test.js: list after readings, the guard, the new pins;
  - buoylinks.test.js: pointer focus;
  - test_index_page.py: note position, CSS, `tellOnPage`, `linkFocus`, the list-landed call.
- Suites: Node 1,013 pass. pytest 1,316 pass + the 4 golden tests, which pass after the re-capture (52 of 52 in the
  overlay-flag and page files).
- Mutants (scratch polish/mut7c.py, on a scratch copy after pytest): 15 of 17 killed. W3 (no guard) was killed only after
  its test's stand-in list was made a real array. The 2 survivors are equivalent: N2 (repeats in the waiting list are
  removed again when shown) and W4 (no feed check: the guard catches it).

## Test-site verification of fix round 7c (UI 1.25.9, 2026-10-25)
`test` @ 703e37e: cherry-picks of b44ba4b and 75b1ab6; the test branch keeps its own README. UI 1.25.9 was served from
03:34:26 UTC, with 0 failed requests across the switch (12 polls). All nine UI files served = the commit, and the
1.25.8 URLs answer 404. The site files equal the feature branch except the test site's three logo lines.

The checks ran in headless Chrome with the GPU, its own profile, never signed in, using trusted mouse, touch and keys.
Scripts are in scratch polish/v7: v2e, v2g, v2h-v2j, v13e, v13f, v9b, v9c, and the step-7 regression scripts; outputs
are in out7c/.

Suites on the test branch:
- Node: 1,012 pass (the feature branch has 1,013; test lacks one production manifest test).
- pytest: 971 pass, plus the same 5 known failures as at step 7 (the test logo goldens and the reef module's numpy
  import).

Answers: the forecast answers for 13 stations and variants are identical before and after the deploy on the same runs
(GFS 2026102418, GLWU 2026102501, SWAN 23 UTC). The removed stations' answers are identical too.

### V7-2: the focus after a link (fixed)
- **One window at a time** (v2e; desktop click and key, phone tap and key): after "Live buoy" the focus is in the live
  window's header, and Escape closes the live window while the forecast window stays. At step 7 the focus stayed on
  the button, and Escape minimised the forecast window behind.
- **Both windows already open** (v2g; 1280x800, 1920x1080, iPad, mouse and touch): the window behind was brought
  forward by a press on a visible part of it, then its link was pressed. The focus went into the window the link
  brought forward, and Escape acted on that window. 12 of 12 checks pass.

### V7-3: the notes (fixed)
v13e ran at 1280x800, phone 375x812, phone 360x640 and iPad 1024x1366: 19 of 19 checks at each size, 76 in all.
- **Kept together:** a removed default (44062) and removed favourites (44062, 46088) are both said, one per line, each
  once. At step 7 the second replaced the first.
- **Position:** the notes sit between the toolbar and the body, on top at their centre. They stay in view while the
  table is scrolled to the current hour (scroll position 198-218 px), and the table's frozen header sits at the body's
  top.
- **Minimised:** the notes are not drawn in the chip, and the chip's box is the same with or without notes.
- **Graph view:** the notes stay above the charts.
- **Another station:** the notes go and the body moves up again.
- **Points:** section 31's "My points is full (50)" note shows on the point's forecast, in view above the scrolled
  table.
- **Two more scripts:** b13b shows both notes together, desktop and phone, with 44062 or 46088 as the default; b13
  (the removed points) is unchanged.

### V7-4: the stall note when the readings arrive first (fixed)
- v9b: 9 of 9 order cases pass. At step 7 the three readings-first cases had no note.
- v9c: the 147 and 146 of 294 thresholds pass with the readings first, and the real answer shows no note in either
  order.
- v9_stall: 15 of 15 rule cases pass.

### V7-1
The two corrected template comments are in the served page; the README change lives on the feature branch (the test
branch keeps its own README).

### Regression
- **b2_links at 1280x800:** the only differences from reviewer B's run on 1.25.7 are the intended ones: the 44-px
  buttons (7b), the focus after "Live buoy" (7c) and the Escape order that follows from it.
- **v13c (46088's note offers its live buoy), desktop and phone:** identical to step 7.
- **b15 (the link buttons' layout), four sizes:** identical to step 7, except the far link's distance to 41002: the
  buoy's reported position moved, 203 to 215 mi.
- **v12 (the station-list retry):** identical.
- **b12:** after a full round, a fresh page target's console is empty.

### Findings
- V7c-1 (P3): printing a MINIMISED forecast window prints its toolbar and table but no longer its notes. In 1.25.8 the
  notes were inside the printed body; an open window prints them in both versions. The notes are about settings, not
  the forecast. Fix options: one print rule (`.forecast-win.fw-min > #fwNote:not([hidden]) { display: block; }`), or
  accept.
- Owner awareness: the notes stay above the table until another station is opened; there is no close button. On a
  360x640 phone, two notes take 122 px (19 % of the screen). They are one-time notes: a removed default or favourite,
  or a point not kept.
- Not a regression: going through the live window's controls with Tab, the browser leaves the page once after the last
  control (the page body has the focus) before the map. The pre-7c build run locally does the same (2 of 2 runs), and
  7c changes no tab order. A run that starts on the last control goes straight to the map.
- Unrelated observation: right after the deploy restart, NWS station 011HI had no MADIS reading. MADIS's public viewer
  answers only the last hour, so a reading kept in memory before the restart is not there afterwards until the station
  reports again.

Cleanup: the storage of both origins was emptied, the browser stopped and its profile removed, the temporary pre-7c
worktree and the two local servers removed, and the in-app browser pane was not used.

## Re-check of the fix rounds (cf49ebb..954153c, 2026-10-25)
Two fresh-context reviewers at MAX, on `feat/map-polish` @ 954153c (fix rounds 7a, 7b, 7c and the 7c print rule) and the
test site @ 72bf2ae (UI 1.25.9):
- R1, code and data (Opus 5.5), no browser.
- R2, the test site and the owner's use (Opus 5.5), headless Chrome with trusted input.
Brief: scratch `g35/recheck/brief.md` (owner decisions 1-10, claims R1-R22, known items K-1..K-9). Reports: scratch
`g35/recheck/r1/recheck-r1-report.md`, `g35/recheck/r2/recheck-r2-report.md`.

**Result: 0 P0, 0 P1, 0 P2, 8 P3 (R1 4 + a test-gap group, R2 4), plus three pre-existing items outside section 52.**
Nothing found that the fix rounds broke for visitors.

### R1: code and data — 0 P0, 0 P1, 0 P2, 4 P3 + one P3 group of test gaps
| # | Finding | Author's check |
|---|---|---|
| R1-1 (P3) | The blank tool can lose a comeback, the mirror of G35 F1. A removed station with data again in both newest runs leaves `removed`. If the scan that completes this is a plain scan (no `--apply`), the station is not listed and not removed, so no later run scans it or restores it. | Confirmed by running the tool's real `main()` offline (R1's `blank_sim.py`): lost in 2 of 4 run orders (`--apply` then a scan; several scans then `--apply`). |
| R1-2 (P3) | MADIS's empty answer (a header, no records) is silent: the kept readings stay, but there is no warning and no error. Seen live on the test site at 04:50 UTC (`last_rows 0`); back to 321 rows by 05:20. | Confirmed offline (R1's `madis_empty.py`: `{}`, no log line, `last_error` None). The F5 warning covers only "records and no wind". |
| R1-3 (P3) | A back link outlives its window: closing the live window keeps `lwBack`, so a later tap on the same buoy's own dot shows the link back to the forecast it first came from. Visible only on 6 asymmetric pairs (e.g. 46247 -> 46026, 41038 -> 41110). | Confirmed by reading: `closeLiveBuoyPanel` and the live window's `onClose` clear nothing. `syncBuoyLinks` drops `lwBack` only when ANOTHER buoy is shown, and the dot click (and Enter / Space on a dot, which fires the same click) reopens through the same `openLiveBuoy`. |
| R1-4 (P3) | A far link to a CMEMS buoy shows CMEMS's file id: "Live buoy IR_TS_MO_6200024 · 1.3 mi away" (and the aria-label). The live window calls the buoy "6200024" (its name). 6 forecast stations show it: 62024, 62082, 62084, 62146, 62163, 62170. | Confirmed: `linkText` / `linkDescribe` use `linkKey(id)`. Every CMEMS buoy in the live list has its platform code as its name (6200024, Gullfaks C ...). |
| R1-5 (P3) | Test gaps: 6 mutants survive and 4 more are caught only by a hash pin or the golden. A3: the socket-cut timer at 8 s instead of `max_s`. W3: the windless warning inverted. W6: no disclaimer for an NWS-API-only window. F10: Escape minimising a forecast window in front with no station window. F12: the forecast event without lat / lon. N15: 1-2 km links not moving the map. N20: an aria-label from 1 km. T8-T10: the blank tool's `main()` untested. | Agreed. A3 can't be told apart with a real socket on Windows (the loop's own deadline cuts a trickle; a stalled read lasts to the read timeout), so the timer's interval is pinned directly. |

R1's observations (not counted):
- The station list's `.catch` also catches errors thrown by the code that uses the list, and asks for the list again for them.
- One-time notes can be lost on a failed reload, and the head's forgotten default is said only once the list lands.
- Same-buoy pairs that the alias table does not cover: CEFAS 8 Sandettie Light Vessel / 62304, CEFAS 10 Clipper /
  62144, CEFAS 305 Lomond / 62146.
- The overlay's controls still key on `(pointer: coarse)`.

R1 confirmed held:
- `NearestCanvas` matched a brute force on the real Leaflet 1.9.4 canvas over 500,000 random clicks and 4,000 hover runs.
- The links matched an independent reproduction on all 563 live buoys and 795 forecast stations.
- The stall note matched an independent reading of decision 6 on 5,000 payloads.
- `_capped_get` on real sockets: cut at `max_s`, capped, short bodies.
- Suites: pytest 1,320, Node 1,013, and the changed Node suites with the clock in UTC and EST5EDT.
- Mutants: 88; 72 killed by tests, 5 by a pin, 5 by the golden, 6 survived (two equivalent).

### R2: the test site and the owner's use — 0 P0, 0 P1, 0 P2, 4 P3
| # | Finding | Author's check |
|---|---|---|
| F-1 (P3, owner's call) | The twin margin (R9) covers a forecast dot you can see. An older design choice (May 2026) draws the live dot 6.5-7.5 px right of its coordinate "so both can be clicked", so a twin at the buoy's spot is drawn as a yellow dot mostly visible beside the cyan one. The "radius + 3 px" rule counts it as under the live dot, so on touch screens the live dot's pad covers it and a tap on the yellow dot opens the LIVE window (production opens the forecast). Seen at Oahu z10 (51201, 51211, 51212) and the North Sea z7 (62145, 62305, BSH03). Over the real lists, of the 94 twins within 1 km: 94 covered at z6-z7, 55 at z10, the 14 co-located ones at every zoom. | Confirmed: `liveTargets` skips a twin within `p.h + LIVE_TWIN_UNDER` (5.5 + 3 px); the anchor offset is in `createLiveBuoyIcon` (`centerOffsetPx`, commit baa40a8). R2's 4x crop shows most of the yellow dot beside the cyan one. |
| F-2 (P3) | A removed default's one-time note is lost when `/stations.json` fails twice. The head removes the default from storage at once, but the note is told only from `forgetRetiredDefault`, which runs only on a successful list. Reproduced at 4 sizes. | Confirmed by reading (R1 saw the same). |
| F-3 (P3) | An old stall note comes back when the wind layer is ticked again (or the list lands while it is off): the note worked out from the last readings shows until the new readings land (110 ms here, 3 s with the readings held). Older than 7c; 7c's work-out on the list landing adds a way in. | Confirmed: `syncWindFeed(false)` stops the feed but keeps `windStallText` and `windLastFeed`. |
| F-4 (P3, accessibility) | Closing a live window opened by "Live buoy" puts the focus on the map, not back on the link. The forecast window does return the focus to its link. Since 7c mouse and touch users get this too. | Confirmed: the live window's `onClose` always calls `focusMapAfterClose`. |

R2 also saw R1-3 and R1-4 on the site:
- after closing the live window, 41159's own dot still links to 41036;
- forecast 62170's link reads "Live buoy NO_TS_MO_6200170 · 11 mi away".

Pre-existing, the same on production (not counted):
- O-1: a tide icon's touch pad is measured against tide icons only, so it covers wind rings 12-22 px away. A tap on a
  wind ring's own centre opens the tide window (Florida z9 KFPR, KVRB, KMLB, KX26; Oahu z10 061HI).
- O-2: at 1280x800 the live window covers the minimised forecast window's expand button.
- O-3: buoy 41002 has not reported since 10/03.

R2 confirmed held:
- 404 trusted taps per site in 8 views: test never opened nothing where production opened something; every difference
  is the touch reach or F-1.
- Hover goes to the nearest dot. The `change` listener was driven by re-keying the rules to `(pointer: coarse)`; a
  simulated computer without a touch screen clicks as production.
- 210 link, focus and Escape checks (click, tap, Enter, Space; 6 sizes; two and three windows): 0 fails.
- The notes at 7 sizes, also with the geo lookup moved to New York.
- The stall rule's edges; R17, R18, R10 and R8.
- Layout at 7 sizes: no sideways scroll, no overlaps, 44-px links.
- A fresh page's console after a full round: 0 problems.
- Timed playback (after R1's DONE; run 2026102500, edge warmed):
  - 1920x1080 shark: test 7.9 / 8.0 fps, production 8.0 / 8.0, 0 unpainted.
  - Live heap: +32.0 / +32.1 MB on test vs +31.8 / +32.0 on production; peaks +50.4 / +48.5 vs +47.2 / +47.4 (within
    the 57.4-MB line).
  - North Sea phone pair: 8.0 fps on both.

Not covered by either reviewer: a real iPad (K-6), a real computer without a touch screen, a screen reader's speech,
real printing, a link pair across the date line (none in the lists).

### The owner's decisions (2026-10-25)
- **F-1, keep as built.** On touch screens the live dot takes the spot, including the yellow twin drawn beside it (the
  May 2026 offset). The forecast is one tap away through "Forecast for this point". The rule's comment and the README
  say so (a twin within 3 px of the dot's edge counts as under it).
- **O-1, fix now.** A tide icon's touch area stops half-way to a nearby wind flag, as the live dots already do.
- **Aliases, add the three:** CEFAS 8 Sandettie -> 62304, CEFAS 10 Clipper -> 62144, CEFAS 305 Lomond -> 62146.

### Fix round 7d scope (UI 1.25.10)
Server and tool:
1. R1-1: `station_blank.json` keeps a `restore` list: the ids that left `removed` but are not on the list yet.
   - They are scanned every run, `--apply` puts them back at their place and empties the list, and an id that goes
     blank again in both newest runs becomes removed again.
   - A test drives `main()` with the network faked through R1's four run orders.
   - A committed-file invariant: every id in a recorded run's blank / back is listed, removed or waiting in `restore`.
   - README, docstring and rule text.
2. R1-2: a MADIS answer with no station records logs a warning (the kept readings stay).
3. R1-5 server gaps:
   - A3: the timer's interval pinned to `max_s`.
   - W3: the log cleared before the windless answer, and no warning after a good one.
   - W6: an NWS-API-only window carries the disclaimer.

Page and client:
4. R1-3: `lwBack` cleared when the live window closes and when a dot opens it (a click, or Enter / Space on the dot).
5. R1-4: a far link to a CMEMS buoy names it as the live window does (its name, the platform number), in the text, the
   aria-label and the title rule.
6. F-1: the comment and README wording only (owner: keep).
7. F-2: the head's removed-default note told at page start, not after the station list.
8. F-3: the stall note and the kept readings forgotten when the wind feed stops.
9. F-4: closing a live window opened by "Live buoy" gives the focus back to that link when it is still shown; else to
   the map, as now.
10. O-1: `tideTargets` measures the drawn wind flags (other than its own gauge's) too.
11. The three aliases.
12. R1's observation: a fault in the code that uses the station list is logged and does not ask for the list again;
    only a refused or broken answer is asked for once more.
13. Tests:
    - one for each item;
    - R1-5's page gaps: F10, F12, N15, N20;
    - R1's surviving mutants re-run;
    - UI 1.25.10 and its fixture; the golden in its own commit; README.

Then the test site (MAX) with R2's scripts for the changed parts, and the owner's go-ahead for production.

## Fix round 7d (the re-check's fixes, 2026-10-25)
Code 0a4ccf6, golden de011a3 (`feat/map-polish`). Not on the test site yet.

| Item | Done |
|---|---|
| R1-1 | `station_blank.json` gains `restore`: the ids that left `removed` and are not on the list. Each run scans them; `--apply` puts them back at their old place and empties it; one blank in both newest runs again goes back to `removed`. Tests drive `main()` with the network faked through the four run orders (two lost the comeback before: confirmed on the old tool), a hand re-add, a renewed blank, and the committed-file invariant. |
| R1-2 | A MADIS answer with no station records logs a warning; the kept readings stay. |
| R1-3 | `openLiveBuoy` forgets the back link (a dot's click, or Enter / Space on it); the "Live buoy" handler sets it after the open; the live window's close (`liveWindowClosed`) forgets it. |
| R1-4 | `linkLabel`: a far live buoy is named by `buoyDisplayLabel` (the live window's own rule; the test harness now runs the template's function, not a stub); text, aria-label and the title rule use it. |
| F-1 | The twin rule's comment and the README say that on touch screens the live dot's pad takes the yellow twin drawn beside it (the 6.5 / 7.5-px offset of baa40a8); owner: kept. |
| F-2 | `tellHeadRetired()` runs at page start. |
| F-3 | `syncWindFeed(false)` forgets `windLastFeed` and `windStallText`. |
| F-4 | `liveWindowClosed`: the focus goes back to "Live buoy" when it opened the window, is shown, and the focus was in the closed window or nowhere; else the map rule as before. |
| O-1 | `tideTargets` measures the drawn wind flags of other stations too (Chebyshev, half-way); a flag on its own gauge still means pad 0. |
| Aliases | `cefas:8` -> 62304, `cefas:10` -> 62144, `cefas:305` -> 62146 (live positions checked: 6.6-13 km from the points, so the far text and the pan apply). |
| Station list | Only a refused or broken answer is asked for again; a fault in the code that uses the list is logged. |
| Tests | A3 (timer interval = `max_s`: points 8 s, MADIS 90 s), W3, W6, F10, F12, N15, N20, plus one per item. |

No `static_ui` file changed, so the UI asset stays 1.25.9 (the standing rule: template-only changes do not bump); the
scope's "1.25.10" does not apply. The golden moves only in the page's script.

Suites: pytest 1,327, Node 1,020. Mutants: 28 of 29 of this round's killed (O2, counting a gauge's own flag, is
equivalent: a flag on its own gauge sets the pad to 0 whatever the distance); R1's survivors T9, T10, W3, W6, N15, N19,
N20 re-run against the fixed code: all killed by behaviour tests (T8, A3, F10, F12 by this round's own mutants B6, A3,
E1, E2).
