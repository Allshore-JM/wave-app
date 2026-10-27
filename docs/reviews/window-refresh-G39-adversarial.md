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

## Step 4: review and fixes
(to follow)
