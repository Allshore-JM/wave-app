# Section 56: the station windows' refresh button (G39 record)

Plan section 56 (C:\Users\joshm\.claude\plans\swirling-spinning-snowflake.md). Branch `feat/window-refresh`, off production
1b57425. UI asset 1.26.3.

## Step 3: test-site verification (2026-10-27, Opus 5.5, effort MAX)

The test site ran `test` @ 057944a: cherry-picks of the five step-1 and step-2 commits (the code, the golden, the test
pins), README kept as the test branch's own. The scripts and logs are in the session's scratch folder
`refresh56/v3` (upstream.py, newer.py, v.js, partial.js, relay_check.py, smoke.js, sheet.py, out/, shots/).

### Deploy
- Pushed 03:49:24 UTC. A watcher asked for /robots.txt and /healthz every 2 s: 130 polls from 03:49:17 to 03:53:36,
  none failed. The old instance answered until 03:50:34, the new one from 03:50:36.
- The served UI assets equal the commit; the 1.26.2 address answers 404.
- The pages differ from production only by the intended lines: the three buttons, their status lines, the CSS, the
  window wiring and the asset version.
- Test-branch suite: 1,196 passed + the 5 failures the test branch always has (its logo lines against the page golden
  x4, the numpy-import test).

### Server: the one-minute rule against the real agencies (in-process, upstream.py)
Every upstream request counted, per route (CO-OPS gauge wind, NDBC buoy wind, METAR wind, the tide water level, NDBC
spectra, AODN spectra).
- A plain request builds (1-12 upstream requests); a refresh at once and four refreshes together ask nothing more.
- 62 s later a refresh asks again (one build), four together ask nothing more, and a plain request then gets the
  refreshed answer (same ETag).
- 62 s after that, with the agency refused:
  - the refresh fails (503, or 404 for the spectra routes);
  - a plain request still gets the last good answer;
  - another refresh at once asks nothing (the failed try counts toward the minute).
  - The CO-OPS gauge is the exception: its history has the NDBC relay to fall back on, so the refresh answers 200 from
    the relay (finding V3-2 below).

### Server: newer data on the test site (newer.py)
A plain request and a refresh, then 7 minutes' wait, then: plain / refresh / plain / refresh.
- The plain request still gave the cached copy after 7 minutes on every route.
- The refresh brought newer readings where the agency had some:
  - Honolulu water level: newest reading 12 minutes newer;
  - CO-OPS gauge wind: 12 minutes newer;
  - Honolulu airport METAR: one hour newer (a new report).
- Where the agency had nothing newer, the refresh brought the same readings:
  - NDBC 51201's spectra (03:00 UTC is still its newest);
  - AODN Wilsons Prom (02:50 UTC);
  - the MADIS-fed NWS station 016HE (no new MADIS reading in that minute).
- On every route, the plain request after a refresh got the refreshed answer, and a second refresh at once was served
  from the server's copy (0.11-0.49 s).
- AODN's spectra took 14.7-15.5 s to rebuild (its THREDDS server); every other route 0.3-2.0 s.

### Browser (headless Chrome 155, own profile, never signed in, trusted mouse / touch / keys)
Sizes: desktop 1280x800, phones 375x812 and 360x640. Stations: buoys NDBC 51201, AODN Wilsons Prom and CDIP 163; the
Honolulu tide gauge (1612340); the Honolulu wind gauge (coops:1612340).
- **Placement.** In all three windows the button sits before minimise and close, the same size as they are (30 px, or
  36 px on touch screens), and nothing covers it. There is no sideways page scroll. Minimised (desktop chip or phone
  bar), the button is gone.
  - Desktop titles are unchanged.
  - On phones a long name loses 44 px, compared with production at the same size: "Pearl Harbor Entrance, Bishop Point"
    267 → 223 px (375 wide) and 252 → 208 px (360 wide); "WHOTS - Woods Hole Ocean Time-series" the same. Short names
    are unchanged. Subtitles shrink by the same amount.
- **A refresh, desktop and phone.** The button is busy at once. The page sends:
  - the live window: its two requests with `cache: 'reload'` and the header;
  - the tide window: its predictions revalidated (`no-cache`, no header) and its water level with the header;
  - the wind window: its history with the header.
  No spinner shows. When it ends the button says "Refreshed." and the view keeps its state:
  - the live window keeps its scroll;
  - the tide window keeps its opened days and its sideways scroll;
  - the wind window keeps the Readings list open and its scroll.
  The focus stays on the button.
- **Two presses 15 ms apart** make one request. A press within the minute gets the same answer, byte for byte (the
  server's copy).
- **Blocked requests** (the refresh's own requests refused by DevTools): the button turns orange with the title and
  status "Could not refresh. Try again."; the content stays as it was, with no error box. The next refresh succeeds and
  clears the orange.
- **A press during the first load's spinner** (that load held) restarts the load: one render, and the held load never
  draws when let go. The button forgets that press (the window re-opens) and says nothing; the spinner shows the load.
- **A window showing an error.** The refresh acts as its Retry and loads the station; no failure is shown.
- **Keyboard.** Tab from the header reaches the refresh button with a visible focus ring. Enter and Space each refresh
  once, and the focus stays. The next Tab goes to minimise.
- **Motion.** The arrow turns while busy. Under reduced motion it is dimmed to 40 % and does not turn.
- **Three windows open.** A refresh asks only for its own station, and the other buttons stay idle.
- **Generic buoys.**
  - AODN Wilsons Prom: drawn in place, its spectra and scroll kept; the refresh took 13-15 s.
  - CDIP 163: drawn in place.
- **The 15-minute rule** (the clock moved on, the tab hidden and shown again):
  - after 14 minutes, no refresh;
  - after 16 minutes, the live and tide windows refreshed once each; the wind window made only its own quiet 5-minute
    request, without the header;
  - shown again at once, nothing more;
  - a tide window parked as a chip (desktop) or a bar (phone) and expanded 17 minutes later refreshed once.
- **A fresh tab** (clean console): the three windows refreshed, desktop and phone. There were no console problems other
  than the failures I injected.
- **Cache headers.** The plain answers' Cache-Control and CDN-Cache-Control are identical to production's on all five
  routes.
- Storage of both origins emptied afterwards; the browser stopped and its profile removed.

### Findings
- **V3-1 (P3): "Refreshed." when only the spectra part failed.** I refused only the spectra request of a refresh
  (partial.js):
  - NDBC 51201 drew the new summary next to its old spectra (the time "05:00 PM HST" unchanged);
  - AODN redrew its old spectra.
  Both then said "Refreshed.". The view stays consistent (the old spectra keep their own time), but the message
  overstates what happened.
  - Cause: the page counts only the summary / bulk part. On a refresh, the server also answers a failed spectra rebuild
    with 404 to the refreshing client, while it keeps its good copy for everyone else.
  - Proposed: when the window shows spectra and the refresh cannot bring them, keep them and report "Could not refresh"
    (the button orange).
- **V3-2 (P3): a refresh can make a gauge's wind older.** Checked in-process (relay_check.py, the floor lowered to 1 s).
  With CO-OPS refused, a refresh of Honolulu's wind history swapped the direct CO-OPS answer (238 rows, newest reading
  03:54 UTC) for NDBC's relay copy (232 rows, newest 03:18 UTC, 36 minutes older), for every viewer, and said
  "Refreshed.".
  - Without the button the same swap happens when the 10-minute cache runs out; the refresh only brings it forward.
  - Proposed: a refresh keeps the answer it holds when the rebuilt one's newest reading is older. This applies to wind
    histories, tide water levels and spectra (by their newest time).
- **V3-3 (owner awareness): Australian buoys refresh slowly.** An AODN buoy's refresh spins for 13-15 s, because AODN's
  THREDDS server is slow; the old spectra stay meanwhile.
- **V3-4 (owner awareness): long names on phones.** A long station name shows about 44 px less on phones (see Placement).
- **Not counted as findings:**
  - A refresh pressed during the first load's spinner says nothing: the spinner shows the load.
  - On a phone an expanded window is full screen and covers the parked bars (as before this release), so a bar's
    expand button is reachable only with the other windows parked or closed.
  - My first runs of three checks failed on my own script, not the page:
    - read too early: the refresh during the spinner; the AODN spectra before they had arrived;
    - a click covered by another window: the tide window's minimise button, under the wind window.
    Each was fixed and re-run.

## Step 4: review and fixes (2026-10-27, Opus 5.5, effort HIGH)

One fresh-context reviewer read `git diff 1b57425..7ab75d1` and checked the test site lightly (5 curl requests, no
browser). Its report and scripts are in the session's scratch folder `g39/r` (g39-report.md, scripts/, mut-out*.txt).
Result: **0 P0, 0 P1, 1 P2, 3 P3**, plus three owner-awareness notes. V3-1 and V3-2 (step 3) were known to it and not
re-reported.

### Findings
- **G39-1 (P2): an AODN spectra refresh ran outside every cap.** Each build is 8 requests to AODN's THREDDS server
  (13-15 s), and the site has 4 request threads. Refreshes of four different Australian buoys at once (or four automatic
  15-minute refreshes) would hold every thread, `/healthz` too, for ~15 s; anyone can send the header. In-process: 8
  refreshes of 8 buoys ran 8 builds at once.
- **G39-2 (P3): a refresh that brings a quiet NDBC buoy back left "No Recent Reports" on screen** above the new readings
  (the refresh path never hid that box; the page test's fake elements did not keep their classes, so nothing caught it).
- **G39-3 (P3): for an NWS station, a refresh during a busy minute replaced a full answer with a thin one.** With the NWS
  budget spent, the rebuild is "ok" from MADIS alone: 102 rows (newest 10 min old) became 19 rows (newest 60 min old) for
  every viewer; the README said a busy answer replaces nothing. Same family as V3-2.
- **G39-4 (P3, tests): 37 mutants, 24 killed, 13 survived, 8 of them real gaps**: T4 (a water level arriving without the
  predictions never drawn), H6 (the content box), H3 (scroll restored on a plain load), C2 / C4 (the status line not
  emptied: a second "Refreshed." not announced), S14 (tide error entry), T2 / T3 (the midnight-refresh lines).
- Owner awareness:
  - A1: most non-NDBC buoys (CDIP, QLD, AusWaves, the European feeds) show their feed's own copy, refreshed every 5-30
    minutes; for them the button only replaces the browser's copy; only NDBC buoys and AODN's spectra are asked of the
    agency again.
  - A2: the 15-minute rule is "data 15+ minutes old when the tab is shown", not "hidden 15+ minutes".
  - A3: a refresh on an error state acted as Retry and moved the keyboard focus to the window header.
- Held up (the reviewer's checks): no path draws a stale answer under another station; the button cannot stay busy
  forever; the server rule (exactly "1", the floor, one build under concurrency, busy caches nothing, keep-last only for
  good answers); the NOAA budgets and the cache headers.

### The fixes (UI 1.26.4; feat/window-refresh @ 482486a, golden 743575a, pin 9d27780)
- **G39-1:** an AODN spectra refresh takes the single slot of `_SPECTRA_REFRESH` without waiting; when it is taken the
  route answers 503 busy with `retry` and is not counted (the button turns orange). Plain requests are unaffected.
  NDBC's spectra are not capped: its summary route already reads NDBC on every request (the reviewer's point), and a
  slow AODN build must not block NDBC refreshes.
- **V3-2 / G39-3:** a refresh never makes an answer worse (`_held_is_better`, wind and tide): a rebuilt history or water
  level whose newest reading is older than the one held, or as new but partial, leaves the held answer for everyone (the
  refreshing client gets it too). A partial wind history is now marked `"partial": true` in the answer.
- **V3-1:** a buoy refresh that cannot bring back the spectra the window shows keeps the whole view and reports
  "Could not refresh" (NDBC and generic buoys); a buoy whose spectra never showed still refreshes its readings.
- **G39-2:** both success paths hide the "No Recent Reports" box.
- **A3:** the refresh button acting as Retry leaves the focus on the button; the Retry button still moves it.
- **A1 / A2:** written into the README.
- **G39-4:** pins for T2, T3, T4, H3, H6 (through G39-2's test), C2 / C4, S14 (with an error time longer than the
  floor, so the rule decides); the page test's fake elements keep their classes.
- Suites: pytest 1,599 (after the golden's re-capture), Node 1,051. Mutants of the fix round: 15, 14 killed (W5 after a
  pin); C3 is equivalent on the real page (a plain load clears the remembered spectra before that line).

### Test-site check of the fixes (2026-10-27, test @ 31c2642)
- Deployed 05:13:34 UTC; new assets served from 05:14:44; 107 polls, one /healthz poll timed out (20 s) during the
  switch while /robots.txt answered. Served tides.js / winds.js = the commit; the 1.26.3 address answers 404.
- V3-1 live: with only the spectra request refused, NDBC 51201 and AODN Wilsons Prom both say "Could not refresh. Try
  again." (orange) and keep their whole view (step 3: "Refreshed.").
- V3-2 in-process against the real agencies (relay_check.py): with CO-OPS refused, a refresh keeps the direct answer
  (238 rows, newest 05:00 UTC) for everyone (step 3: swapped for the relay copy, 36 minutes older).
- Regression on 1.26.4 (headless Chrome, trusted input): the flows desktop + phone, two presses, blocked refreshes,
  AODN + CDIP buoys, the 15-minute rule (incl. the chip), a fresh tab with the three windows refreshed on desktop and
  phone: all pass, console clean.
- G39-1 is covered by the route tests (a live check would cost 16+ THREDDS requests and stall the shared test instance).

## Step 5: production (2026-10-27, Opus 5.5, effort MEDIUM)
- Rollback tag `prod-pre-refresh` @ 1b57425 pushed; `Live-Buoy-Update` fast-forwarded 1b57425..a58e14a (UI 1.26.4).
  New assets served from 05:26:33 UTC; 127 polls of /robots.txt and /healthz across the switch, none failed.
- forecast.js, tides.js and winds.js served = the commit. The 51201 and 51003 pages differ from the pre-merge capture
  only by the section-56 markup, CSS and scripts and the asset version, and equal the test site's minus its 3 logo
  lines (test-only logo count 0).
- Live: a refresh of coops:1612340 rebuilt (0.70 s), a second within the minute was served from the copy (0.14 s, same
  ETag); plain answers 200; /healthz ok. CI on a58e14a green (coast-build, model jobs).
- Headless Chrome on allshoresurf.com: the refresh flows desktop + phone, two presses: all pass, console clean; storage
  of both origins emptied.
- Rollback: re-push `prod-pre-refresh`.

