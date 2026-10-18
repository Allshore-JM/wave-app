# NOAA GFS Wave Table View Web App

This repository contains a Flask web application that fetches the latest NOAA GFS wave `.bull` file for any available buoy station, parses the data, and displays it in the same format as the **Table View** worksheet from an Excel file provided by the user. The output includes a metadata row, two header rows (parameter names and units), a blank row, and the data rows.

## Features

- **Buoy Dropdown**: The home page includes a dropdown menu preloaded with all NOAA GFS stations available in the `.bull` directory. You can select any buoy station to view the latest model data.
- **Automatic Run Detection**: The application automatically determines the most recent model run (18z, 12z, 06z, or 00z) by probing the NOAA directory structure for the selected date and hour. If the latest run isn't available, it falls back to earlier runs.
- **Excel-Style Table**: The data is formatted to mimic the two-level header structure found in the provided Excel **Table View** sheet, including a metadata row for cycle information and units row for each parameter. A blank separator row is also included for clarity.
- **Download as Excel**: You can download the displayed table as an Excel file. The download preserves the two-level headers and units row.
- **Deployment-Ready**: The repository includes a `requirements.txt` file and a `README.md` with instructions for deploying the web app on [Render](https://render.com) or running locally.

## Getting Started

### Local Development

To run the app locally, first install the dependencies and then run the Flask server:

```bash
pip install -r requirements.txt
python app.py
```

The app will be available at `http://127.0.0.1:5000` in your browser. From there, select a buoy station to load the latest wave forecast and view it in table format or download as Excel.

### Deploy to Render

Follow these steps to deploy the web application on [Render](https://render.com):

1. Create a new account or log in to your Render account.
2. Fork this repository or push the files in this directory to your own GitHub repository.
3. In Render, create a **New Web Service** and connect it to your GitHub repository.
4. Set the following options:
   - **Environment**: Python 3.x
   - **Build Command**: *(leave blank)*
   - **Start Command**: `gunicorn app:app`
5. Click **Create Web Service** and wait for deployment to complete. Render will build your app and provide a public URL.

Once deployed, navigate to the provided URL to access the app. The site will allow you to choose from the list of available buoys and view the latest data.

## File Structure

- `app.py` — The main Flask application. It includes the routes for the home page and Excel download, the logic to detect the latest model run, fetch `.bull` files, parse them, and format the output.
- `requirements.txt` — Lists the Python dependencies needed to run the app (`Flask`, `pandas`, `requests`, `openpyxl`, `gunicorn`, `pytz`).
- `templates/index.html` — Jinja2 template containing the HTML structure for the home page: the map (the page), the Leaflet map and the forecast window (see below); Bootstrap for styling.
- `static_ui/forecast.js` — the forecast window's client module (see below); `static_overlay/` — the optional model overlays.
- `README.md` — This file. Provides setup instructions and describes the features of the project.

## Contributing

Contributions are welcome! If you want to add new features, improve the parsing logic, or update the UI, please submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## Forecast window client (static_ui)

`static_ui/forecast.js` is the page's own client module for the forecast window (plan section 25). It is served at
`/ui/<name>?v=UI_ASSET_VERSION` (app.py), always (not behind the overlay flag, never under `/overlay/`), immutable for a year
like the overlay assets: any other `?v` is a 404 with `no-store`. Bump `UI_ASSET_VERSION` on every change to `static_ui/*`
and update `tests/fixtures/ui_assets.json` (version + sha256); `tests/test_ui_module.py` checks the route, the pin, and runs
the Node tests in `tests/ui/`. `/api/forecast` also reports `model` (the model actually used: SWAN falls back to GFS off the
12 SWAN stations) and `swan_available`; `?compact=1` asks for the window's table.

The page (2026-09-26, plan sections 25 and 26): the map is the page — the brand sits on it top-left above the
model-overlay selector, the settings gear (Time Zone, Units, lat/long gridlines; with the model overlays also "Pressure
isobars (wind)" and a "Particles" slider, plan section 46: see below) top-right under the layer legend, the
credits behind an (i) beside the Home button; the forecast window's heading is the station picker with favourites
(expanded and minimised); the live-buoy panel is a second floating window (`#liveBuoyPanel`, `createLiveWindow` in the
module: dragged, resized, minimised to a chip at the bottom-right or a bar above the forecast bar on phones, closed
from any mode; `sessionStorage 'allshore.liveWin.v1'`). The forecast is a floating window (`#forecastWin`) that starts minimised in
a new tab (a chip at the bottom-left on desktops, a bar along the bottom edge in phone mode = a viewport 500 px or
less wide OR tall; expanded it is a window on desktops and full-screen in phone mode), holds the View (Table / Graph) and Model (GFS / SWAN, only on SWAN stations) controls
in its toolbar, and is dragged by its header, resized from any edge or corner (the bottom-right grip also takes the arrow keys), maximised below the map's top-right controls and minimised;
Escape closes the live-buoy panel first, then minimises the window. Every change (a marker or favourites pick, model, view,
time zone, units) applies in place: the module fetches `/api/forecast?compact=1` (one request per change, older responses
dropped, a 10-minute client cache of 16 forecasts; a failed build is never cached) and rewrites the address bar with
`?station=` (tz and unit whenever they differ from the viewer's saved settings, so the address always reloads to the view on
screen; model / view when not the default), so links and bookmarks keep working and the page never reloads (the map, the
overlay and the live panel stay as they are; every landed forecast is announced as `allshore:forecast`, which the overlay
uses to follow the table's zone and run). A marker pick goes back to Buoy Local, a favourites pick keeps the zone, as before. State
precedence on load: the URL, then the viewer's saved settings (`localStorage 'allshore.settings.v1'` {tz, unit}), then the
server's defaults; the window's geometry and mode live in `sessionStorage 'allshore.forecastWin.v1'`; the charts' range in
`sessionStorage 'chartRange'` as before. The table is the compact form (short dates with the full date on hover, no info
rows: the window shows Cycle / Location / Time Zone in one line above it), about 1,000 px wide for its 23 columns. The
page is a shell on every JS load (the window fetches the forecast; the `#forecastLoading` placeholder stays until it
lands, which the overlay's restore waits for); `?render=full`, reached by the `<noscript>` meta refresh, renders the
forecast inline in the window's compact form and the module shows it without a fetch; without JS the window is a plain
card in the page flow with a Go button in the window's toolbar. The map fills the viewport (on phones less the
minimised bar) and re-applies its stored view only when the width changes (a phone keyboard must not snap it back). The
page golden (`tests/fixtures/index_golden.json`) was re-baselined at this restructure (see `tests/test_overlay_flag.py`).

## Where the site opens (plan section 46, UI asset 1.23.0)

A fresh tab opens at the visitor's own area: the head asks the geo Worker (`tools/geo`, `GET https://geo.allshoresurf.com/`,
Cloudflare's `request.cf` position for the IP address: the ISP's city; nothing is stored anywhere) and the station list, picks
the station nearest that position (haversine, so the date line is no seam) and starts its forecast request; the map opens
centred on the visitor at zoom 6, zoomed out until that station is in view (15 % padding; never below the map's minimum
zoom), and the forecast window loads the station. The Worker's answer and the list are raced against a 2-s cap; while it runs
the map pane is hidden (no Hawaii tiles paint; the navy background shows under the controls) and a visitor who moves the map
or picks a station meanwhile keeps what they did. Every failure (no Worker, a slow or refused answer, a VPN exit on land far
from the sea) lands on today's behaviour: Hawaii and 51201. Precedence: a station in the address (`?station=`: every reload,
since the address is written after the first load, and every link) > the viewer's default station (the blue star in the
favourites picker, localStorage `allshore.defaultStation.v1`: its position at zoom 6) > the IP's nearest station > 51201;
the picker's star cycles: empty, then gold (a favorite; for a point: kept in My points), then blue (the default: one
at a time, listed first; a new default leaves the old one a favorite), then empty again (neither); removing the
default's favorite row or its point clears it as the default too;
for the map: the tab's own view (sessionStorage) > the default station's position > the IP position > Hawaii. The head and
the module ask for one and the same query (tests/ui/early.test.js); the module's first load waits for the head's answer
(`window.__early.station`) and a load the viewer causes meanwhile wins. Env `GEO_BASE` (e.g. `https://geo.allshoresurf.com`)
turns it on; unset, the page opens as before. The Home view button is gone (its saved view is no longer read).
G32 fix round (UI asset 1.23.1): while the race runs nothing is kept as the tab's view (a reload then asks again); a
pointer, wheel, key or touch on a control over the map (the overlay panel, the gear, the legend, the overlay's sheet) is not
"moving the map", and moving the map shows it at once; the default station's id follows one rule everywhere (trimmed, 1-32
letters, digits, `_` or `-`: a NOAA id or a point's `pt_` id); a default the station list no longer has (and not a point) is
forgotten once the list is in, and the window says so once; the chip names the race's station as soon as the race answers.
While a map tool's bar is open, the wide forecast window narrows to leave it free (owner) instead of being minimised.

## Map tools (static_ui/tools.js)

A ruler button left of the settings gear (plan section 29) opens three tools. While one is active, map clicks, and clicks
on forecast points and live buoys, go to the tool, and double-click zoom is off. Escape clears, then closes (only when
nothing else has focus, or focus is on the tools button, the gear with its panel closed, or a marker). A tool bar under
the gear shows the result, with Undo / Finish / Clear under its title; it never runs past the map's bottom edge (its text
scrolls). An expanded window that the bar covers is minimised when a tool starts and whenever the bar grows taller than
it has been since then (keyboard focus stays where it was). A press that
begins on a control or a window and is released over the map is not a map click. On touch, two taps within 16 px are one
double-tap.

- **Measure distance / area:** great-circle geometry on a sphere (R = 6371.0088 km), in the gear's units plus nautical
  miles. An outline that crosses itself is flagged.
- **Swell exposure:** click the water. A fan of 72 five-degree wedges shows which directions swell can arrive FROM.
  - **Clear with a cyan rim:** open.
  - **Light grey:** partly shadowed, for example a distant island (Kauai seen from the North Shore, ~150 km).
  - **Dark grey:** heavily shadowed, for example the spot's own coast.

  How it works:
  - Each wedge is sampled by ten great-circle rays running to the first coastline crossing or 3,000 km.
  - The coastline is the GSHHG data the overlay publishes (`static/coast/v1`). Tier 1 is used for the first 50 km,
    tier 0 beyond. The builder's cell-clip edges cancel by direction along each cell line.
  - A ray's land shadows fully within 15 km and fades with the log of distance, to nothing at the spot's reference
    distance. Land beyond 600 km reads open by itself (at most 0.18) and fades to nothing at 1,000 km (owner: New England
    no longer greys Cape Hatteras's NNE). The reference is the 90th
    percentile of the rays that leave the spot's own coast (100 km minimum, so small bays and sounds read sheltered),
    so enclosed seas adapt; it is the full 3,000 km when 15 or more rays reach open ocean, blended in between from 5.
  - Windows are named by the compass point of their centre, or of both ends when 45 degrees or wider ("NNE–SW
    (015°–230°)"); a spot open on 300 degrees or more reads "Open except …".
  - Clicks on land, or within 150 m of the shore, are evaluated 150 m off the shore (the shoreline data is good to
    ~50-100 m). A land click walks to the nearest coast (giving up just past it when it only grazes the tip of a cove)
    and then straight out to sea along it; a water click moves away from the nearest coast. Unless that first walk
    reached a point 150 m out that looks well out (8 of 16 directions run 2 km without meeting land), 24 directions are
    also searched, up to 2 km on land and 300 m on water. The nearest point 150 m out that looks out well wins (6 of 16
    directions: a bay beats a pond or an inner sound), else the nearest that looks out at all (3 of 16), else the
    nearest point 150 m out; else, of the water found, the nearest that looks out, else the nearest at least 80 % as
    clear as the clearest. A walk in the water stops at its far shore (a walk on land steps over water narrower
    than its 20 m step). A point on the coastline itself is never used, a land click never settles within 5 m of a
    coast, and with no such water within 2 km it is refused. Coast edges are kept in 100 m buckets (an edge only in
    the buckets it crosses), so placement stays around 10-30 ms in the densest estuaries.
  - The fan keeps a fixed size on screen and is panned clear of the map's controls and the windows, preferring a
    smaller fan to a pan of more than a third of the map. When nothing is clear it takes the least-covered spot, never
    centred under the tool bar; the overlay's phone sheet counts as a control. On a map shorter than 550 px the overlay
    panel's details fold when the exposure tool starts. After a resize or a rotation, a fan that was on screen before it
    is placed again; one the user panned away from is only resized. Zoomed out (below zoom 5.75) the fan is a 44 px
    compass, not moved into view; it grows back from zoom 6.25 (the band between keeps either size).
  - **The window on the map** (plan section 30). Each ray also runs on past 3,000 km to the first coast however far
    (tier 0 worldwide, built once a session in slices), to the map's limits (84 N / 79 S) or just short of the
    antipode. Drawn above the wave colours and particles, below the gridlines and stations, in three world copies round
    the spot's copy nearest the view (the fan follows the view across worlds, as the stations do):
    a ray every 5 degrees that runs over 300 km, a 55 % veil over the water the spot cannot see, and a lighter 25 %
    veil behind small islands (a shadow at most 2.5 degrees wide, with farther rays on both sides, from land more
    than 50 km away). Dashed range rings are drawn only inside the window, as arcs that end on its edges, with their
    labels on the arcs (the widest arc, and any other 15 degrees or wider); every 1,000 nm (2,000 km in Metric) when
    two fit, else every 500 / 250 / 100 nm (1,000 / 500 / 200 km), out to the farthest the window reaches, ten at most.
    The window has its own canvas with up to half a map of margin, so a drag does not run past it before it is
    redrawn; the canvas is kept under 12 million pixels (Leaflet doubles it on a retina screen; iOS Safari draws nothing
    on a canvas over 16.7 million), and it is freed when the tool closes.
    The world tier (~1 km) leaves out the smallest islets, and reefs and atoll rims are in no tier, so the shadows of
    atoll chains such as the Tuamotus and the Marshalls read more open than they are.
  - **Readout:** pointing at the map (off the fan) shows the bearing and distance from the spot, whether that water's
    swell can reach the spot ("In the window", with "(partly / mostly shadowed direction)" for a grey wedge; "Behind
    a small island N out" in a lighter strip; "Blocked by land N from the spot: no swell from here"; "Not in the
    window: its path to the spot crosses the map's polar limit"), and deep-water travel times at 14
    and 18 s (in hours under a day), with a dashed line on a dark casing along the great circle to the cursor. A
    keyboard pan or zoom reads the point under the pointer again. Over the map's controls nothing changes. On a touch
    screen, Lock and tap: the tapped readout stays until the next tap (also when a window opens over the map), and a
    tap's compatibility mouse events never start or move it. A tap on a wedge, on the full fan or the compass, shows
    that wedge; a tap on the compass's centre does nothing.
  - **Lock** keeps the window on the map and gives the map back to the page: forecast points, live buoys and
    double-click zoom work again, and a click only moves the readout. Escape then acts only with focus in the bar, and
    there it only unlocks; Unlock also ends a tapped readout. Clear, closing or another tool unlock too.
  - **The tool bar** folds to its title row with its ▴ button; under it still show a readout, the tool's messages
    (a refused click, the coast data unavailable, a window that could not be drawn), "Computing…" while busy and
    "· locked" while locked. On a phone (a map narrower than 576 px, or shorter than 400 px on a touch screen), once a
    result shows, the bar leaves out its explanation line; the status lines ("Moved … off the shore", "lower detail")
    stay. Every readout fits the details line's three lines at the default text size (the sentences are pinned to 79
    characters); at larger text sizes the line grows rather than clipping, and that growth never moves or minimises the
    windows. Only the line that changed is rewritten, and a readout that follows the pointer is not announced by
    screen readers (a tapped one is, and so is Lock).

  Limits (also said in the tool bar):
  - It is geometry only: swell wraps headlands and islands, so point breaks can show their main swell as shadowed.
  - Reefs and shallow banks are not in the data.
  - Lakes count as land.
  - Clicks near a marker land on the marker's position (Leaflet).
- **Coast data address:** env `COAST_BASE`, else derived inside the overlay's gated block from `MODEL_FRAMES_BASE` when it
  ends in `/gfswave/0p25/v1`. With neither (for example the overlay flag off and no `COAST_BASE`), the exposure item is
  hidden and the measuring tools still work. Nothing is fetched before the first exposure click. `index.json`,
  `world-i.bin` and 1-4 tier-1 cells follow, cached in memory (8 cells, LRU).
- **Tests:** `tests/ui/tools.test.js` (pure functions, synthetic coasts and the Hawaii crop in `tests/fixtures/coast`, with
  the owner's examples pinned) and `tests/ui/tools-ui.test.js` (the tool state machine on a fake Leaflet map).

## Model overlays (optional)

Animated NOAA GFS-Wave / GFS frames drawn under the forecast and live-buoy points. Off unless BOTH
environment variables are set on the web service:

- `MODEL_OVERLAYS=1` - renders the selector control. With it unset (the production default) the page is
  byte-identical to the pre-feature page (`tests/test_overlay_flag.py` replays a golden capture).
- `MODEL_FRAMES_BASE` - public URL of the frame bucket prefix, e.g. `https://<host>/gfswave/0p25/v1`.

The browser talks to the bucket directly; the Flask worker only serves two immutable assets
(`/overlay/overlay.js?v=...` and `/overlay/overlay.css?v=...`). Bump `OVERLAY_ASSET_VERSION` in `app.py`
on every change under `static_overlay/` (any other `?v` is a 404 that is never cached).

Frames come from `tools/model_frames` (GitHub Actions `model-frames.yml`, cron every 10 minutes, plus manual
dispatch with `steps` / `dry_run` / `force`): NOAA open S3 byte-range GRIB records -> eccodes -> 8-bit PNG
(`u8-linear-v2`) -> Cloudflare R2. Bucket layout `gfswave/0p25/v1/<RUN>/{half/}<field>/fNNN.png`,
`manifest-<ts>.json`, `stats-<ts>.json`, `latest.json` (written last, only for a complete run) and
`failed/<RUN>.json`. The 0.25 degree grid (~28 km) is sampled per map pixel; smoothing between grid points
is not extra detail.

Runbook: rotate the R2 token in the repository secrets; roll back by unsetting `MODEL_OVERLAYS` (env only,
no deploy), by re-pushing the `prod-pre-overlays` tag, or by disabling BOTH workflows (`model-frames.yml`
and `model-frames-keepalive.yml`, which would otherwise re-enable the job on the 1st and 15th; the last
complete run stays live and the panel shows a stale banner after 9 h). Integrate site changes into
`Live-Buoy-Update` by MERGE, never by pushing a feature branch over it: the job-only merges live on the
production branch alone. The workflow step summary states how long after the cycle each publish happened;
`failed/<RUN>.json` and `notready/<RUN>.json` in the bucket record build failures and listed-but-missing
objects. Data policy in the browser: the 0.5 degree frames are used below zoom 3.5 on desktops, below zoom 7
on narrow (phone) maps and for wind everywhere, so an 81-frame loop is about 4-13 MB (32 MB only for wind
zoomed past 7.5); frames are immutable, so a second loop costs nothing. Frames are decoded without a canvas
where the browser has DecompressionStream.
Overlay state (asset 2.7.6): the chosen layer, its valid time and whether it was playing are kept in the tab's
sessionStorage (`allshore.overlay.v1`, with opacity, speed and the contours setting), so picking another forecast
point (a page reload) brings the overlay back where it was. The time and play state come only from a save under 30
minutes old, counted from when the page was left (hiding or leaving the page refreshes the save, paused or not), never
under the viewer's reduced-motion setting, and a hidden tab resumes when shown. The restore starts after the load
event (5 s at most), a deferred forecast table (3 s at most) and an idle moment, with "Loading" in the panel
meanwhile. Off, a new tab, or a layer picked from Off starts without a saved time; switching layers while the saved
one waits to be restored keeps the time; a page shown again from the back/forward cache writes its own state back to
the tab.
The model dropdown (asset 2.17.0, plan section 43): with `MODEL_OVERLAY_MODELS` set to a comma list of the models that have
frames in the bucket (`gfswave`, `ecmwf`; the first is the default; unset = the GFS frames alone and no dropdown), the
open panel's head line is a dropdown of the models and the ECMWF frames (job: the section below) can be chosen. Each model
has its own prefix under the bucket root, its own pointer, run cadence ("Next Update" one cycle on: 6 h for GFS, 12 h for
ECMWF), stale line (9 h / 15 h) and map credit ("Overlay: ECMWF CC BY 4.0"; "ECMWF" opens the site's notice
`/notice/ecmwf`: copyright, source, the CC BY 4.0 licence and link, ECMWF's disclaimer and what Allshore Surf changed); the
table note under ECMWF reads "the forecast table is on GFS-Wave run X". A switch keeps the requested time and the play
state (also under reduced motion: it is the viewer's own playback): the run in hand goes first (downloads aborted, caches
and the picture dropped, never one model's picture under another's name; under wind the relief stays), the new model loads
through the one mount path, and the time and play state are carried to the first frame that lands (a failed load keeps
them for Retry, a field switch meanwhile or a switch back). The time is the one the viewer chose: consecutive switches
return to it until the viewer moves the time (seek, step, play, Update). Cache keys carry the model and the run. The choice
is saved for the tab (`model` in the session key beside the field and the speed) when the new model's first frame lands,
so a load that never ends cannot trap a reload on it; a new tab starts on the first offered model; a saved model that is
no longer offered falls back to it. The loading and error panels carry the dropdown too, and every pointer or manifest
request (a switch, the first load, a field switch's re-read, Update) not complete within 15 s, headers and body
(`LOAD_STALL_MS`), ends in the error panel, so a slow or failing model can always be left. Every one-line
head (the viewer's fold, a short map, a phone held sideways) shows the summary line with the model ("Thu, Oct. 15, 11:00
AM HST · ECMWF"), never the dropdown; the dropdown keeps the head line's height. Frames, scales, legends, contours and
particles are the same code for both models (assets 2.17.1-2.17.5: the G30 fix rounds and their checks,
docs/reviews/overlays-G30-adversarial.md). A load that fails leaves nothing of an earlier run drawn and gives the page's
imagery back (also after a failed switch on wind); an Update drops the pending step at once, so a frame of the run it
replaces can never land over its loading panel.
The settings gear's overlay rows (asset 2.19.2, plan section 46): "Pressure isobars (wind)" turns the isobars over the
wind overlay on or off (on by default; localStorage `allshore.isobars.v1`, "0" = off), and the "Particles" slider
(0-100, localStorage `allshore.particles.v1`) fades the particles: 100 is the look before it, lower values fade the
whole particle layer by one CSS opacity, 0 turns them off (nothing animates and no direction frames are fetched).
Both are remembered in this browser and apply to every field and model; the rows are built by the overlay script, so
they exist exactly when the overlays do. The gear's panel keeps its 260-px width and, on a short map, scrolls inside.
The isobars' look (asset 2.19.3, the G32 fix round; the owner asked for Windy's look): the pressure field under the view is
averaged into blocks at least 7 px apart on screen and smoothed twice by a [1, 2, 1] kernel, so the lines are smooth and the
small rings of mountain pressure reductions and 8-bit terraces go (on a real GFS frame the large systems' lines stay within
55 km of the true isobars for 95 % of their points and within 110 km for 98.6 %; deep lows read a little shallower in the
lines, the readout's "· 1004 hPa" stays the exact value at the cursor). Spacing: every 8 hPa while the whole world is in
view (any screen width) or to zoom 2.9, every 4 hPa to zoom 5.9, every 2 hPa beyond. Each isobar has at most one small
pill (9-px text), rotated along the line, from zoom 4 and never while the whole world is in view; highs and lows are marked
H and L with their pressure, found on the frame's own values (not the smoothed lines): a node that nothing within ~110 px
on screen undercuts (L) or overtops (H) and that is at least 1 hPa (a quarter of the interval at the wider views) from its
surroundings; a flat-bottomed centre is marked once, at the middle of its patch of equal values (no mark for a patch that runs
down to a lower value, holds more than 4,096 nodes or spans half the world); of two equal centres within ~110 px of each
other only one is marked. So a compact typhoon keeps its real centre and central pressure (asset 2.19.4: on the smoothed
field a 956-hPa typhoon read "L 1000" at the world view and two tropical cyclones had no mark), and a second low along a
trough keeps its L (asset 2.19.5: a 998-hPa low 10 hPa deep in the Bering Sea had none: a low of the same pressure 8 deg away
hid it). Lines are thin and faint (a 1.2-px white line at 72 %). A step
waits at most 1 s for its pressure frame (owner, G32): a slower one lands without lines, which appear when the frame
arrives. One frame (at most ~16 ms) of the previous step's lines can show under the new picture when the browser paints
between a landing and the next animation frame (G32 A-P3-5, accepted).
Late frames (asset 2.14.12): the 5-s watchdog (or 4 playback intervals) times a frame's download only; a download that
stalls is asked again at once, three attempts in all, and only then is the frame left out for a minute and listed as
"Unavailable frames are skipped". A frame that is merely slow (a busy page at fast speeds) is waited for, never listed.
Fast playback (assets 2.15.0-2.16.6, plan section 42): the fish and the shark reach their rates (4 and 8 frames/s) on screens up
to 1920x1080 because (1) the ring of decoded frames follows the speed: paused and at 1x the G3/G4 budgets stand (the
frame on the map, 2 ahead, 2 behind, at most 2 downloads at once, at most 5 decoded frames per kind); at 2x the ring is
4 ahead / 1 behind with 3 downloads and 7 decoded, at 4x 6 ahead / 1 behind with 4 downloads and 9 decoded (the downloads
are one budget for field and direction frames together; the decoded cap is per kind; with the isobars on under wind
a third kind, the step's half-resolution pressure frame, rides beside them through the same scheduler and the download
budget grows by one while it is wanted, 3 paused and at 1x up to 5 at 4x, so the target's three downloads start together
rather than the pressure waiting for a landing: asset 2.19.1, plan section 46), so a step never waits for its own
download and a ring frame is never evicted before it is drawn (at 4x
the old 5-frame cache fetched every frame about 1.8 times); every landing refills the ring, not only the target's;
a pause or a slower speed shrinks the caches and the downloads back to the budget at once, never dropping the target's
own field or direction download; a step whose frame is decoded already keeps every ring download ahead of it, and when a
new target leaves too little room the farthest ring downloads go, never the nearest (2.15.1: the old rule reserved a slot
for a decoded target and dropped the oldest download, usually the next frame needed); with a pending target far from the
frame on the map (a seek into a cooldown) the ring is filled only up to the budget, so nothing is ever fetched back and
forth; the frame on the map (and the animator's direction) is kept by value as well as by key, so after a zoom across a
resolution threshold the old-resolution frame still drawn is never evicted (2.16.4); (2) the cadence is drift-free: each
frame is due one interval after the last one was DUE, not after it landed (timer latency and the particles' frames no
longer add up: the snail showed 1.92 of 2 frames/s with everything cached); a frame that lands late is followed by its
successor as soon as the browser has rendered it, one back-to-back frame, never a skipped one: the PAINT GATE (2.16.4, G29):
the tick after a landing runs from a task scheduled by the first animation frame after it, never before (a landing is put on
screen by the rendering update that follows the animation-frame callbacks; a successor landed in the very next task replaced
a late frame unseen, 3-12 % of the frames at 1080p where the GPU renders only ~8 times a second with particles), and without
an animation frame for 250 ms it runs anyway; (3) a new frame draws the tiles in view first and the
buffer tiles Leaflet keeps around the view in idle time (requestIdleCallback, else one task), and a drag that brings
a buffer tile into view draws it at once; the readout samples the frame itself and the particles read only the land
masks, so a buffer tile a frame behind is never read.
Step 3 (asset 2.16.0): (4) the frames could be DECODED OFF THE MAIN THREAD; since 2.16.5 (owner, 2026-10-14) the main thread
decodes by default and the worker runs only with `?ovdecode=worker` in the address (a diagnostic): with the fix round's
leaner decoder (the zlib pieces straight to the inflater, one output buffer, rows unfiltered in place) the main thread reaches
the 1080p rate as well (7.9-8.0 frames/s at shark, idle and under a 25 % CPU load), and the worker's thread and isolate cost
a fixed ~17-20 MB of the tab's memory, paused or not, for only 6-15 ms of main-thread time per frame. The worker, when asked
for: a Web Worker built from the module's own decode functions (one asset, one version) inflates and unfilters each
downloaded body (transferred, not copied) and hands the code array back; the main thread keeps the download, its watchdog
and the late-frame rules. The worker keeps nothing per
frame (2.16.4, G29): the zlib pieces go to the inflater as they are, its output is read into one buffer of the expected size,
the rows are unfiltered in place, and every buffer the decode held (the body, the inflater's chunks) goes back to the main
thread with the code array, where that garbage was always collected before the worker (left in the worker's own heap it was
collected only at V8's ~64 MB trigger, which broke the memory rule once the worker was counted). A worker that dies ends its
pending decodes, which are downloaded once more (edge-cached) for the main-thread path, and stays off for the session
(one console warning); without Worker / Blob / URL.createObjectURL / DecompressionStream the main thread decodes. (5) The particle lattice is rebuilt from the model nodes around the view
plus a screen on every side, not the whole grid (the same vectors on screen; a pan beyond the margin recomputes).
(6) Particles: their pace follows the playback speed (x2 at fish, x4 at shark; x1 paused or at snail), so at the fast
speeds they keep up with the time-lapse instead of drifting behind the storms (on the hourly frames to +120 h; the 3-hourly
frames after that move three times as far per step), while their tails span the same flow time at every speed and frame
rate (a tail point older than 11 sampling intervals of flow is left out: a slow particle loop at the fast speeds drew tails
up to twice as long, 2.16.4; owner: the same length everywhere) and their lifetimes stay in real time; their look is the
owner's pick of 2026-10-13 from four measured candidates:
the density of before (one particle per 30x30 px: a denser look cost the 1080p frame rate), 2-px cores over 4-px halos
(wind 1.6 / 3.4) and brighter tails (55 % instead of 40 %). (7) A
2-second watchdog restarts the animation when it should be running and is not (its data and canvas present, the page
visible, nothing in progress on the map, yet no frame drawn or a pause that never ended; a map "busy" for 10 s with no
map event in between counts as a flag stuck true), shows the canvas again (a zoom whose zoomend never came left it hidden) and, on WebKit (Safari,
and every browser on iOS / iPadOS, which are all WebKit), replaces the animation canvas by a fresh element, as after every
zoom there, since the iPad lost its particles after a pinch zoom until a reload (not reproducible in Chromium); a browser
that refuses the new canvas a drawing context (Safari with its canvas memory used up) keeps the old canvas, and without any
context the animator draws nothing and the watchdog asks again, never an exception into the map's events or the playback
(2.16.4, G29); `?ovdebug=1` writes the animator's and the scheduler's state into the panel once a second, in the error
panel too. (2.16.1: these address parameters are read from the navigation entry's original URL as well,
because the forecast loader rewrites the address to the station query before the overlay module loads.)
Wave-height colours and contours (asset 2.7.6): wave height uses value knots so 0-3 m fills about half of the
legend (owner-picked spectral palette; the knots stretch to whatever legend the manifest carries; a run whose legend is exactly 0-60 ft = 0-18.288 m uses the wider scale in `SCALES`: 0-30 ft unchanged, 40 ft pale pink, 50 ft rose, 60 ft deep purple, plan section 27); tp and wind keep
linear scales. Contours (wave height and period, always on since asset 2.12.0; no checkbox) are light
anti-aliased lines every 2 ft / 0.5 m and every 2 s (doubled below zoom 4), about 1.8 px wide at zoom 8 and closer,
thinning to about 1 px at zoom 3 and below where they are densest. The lines are traced on a lightly
smoothed copy of the frame ([1,2,1] over the model nodes, each node held within half an 8-bit step of its own value), so
they do not follow the 8-bit terraces of flat seas and still sit where the colours and the hover readout put their
level (next to islands as in open water); the colours, transparency, the coast clip and the readout keep the raw
values. No line beside missing data,
where lines would crowd under 3 px apart, along the flat foot of a steep ramp, or in a model cell where the peak
period jumps more than 2 s between neighbouring nodes (the smoothing never blends two swell regimes).
Animation (asset 2.9.5): an Animation checkbox on the Opacity row (off by default, remembered for the tab) draws
particles with fading trails that flow along the dominant swell direction under wave height and peak period, and along
the wind under wind speed, on a canvas of their own under the forecast points (no pointer events; markers keep their
taps). The directions come from the `pdir` / `wdir` frames of the same step, loaded through the frame scheduler
beside the field frame (the target's field frame first, then its direction, then the ring of frames around it, never more
downloads at once than the speed's budget, two when paused or at 1x; `pdir` at the 0.5 degree frames at the default zoom 6 and wider and the 0.25 degree ones as
soon as the map is zoomed in, from 6.5 (back below 6.25), so near coasts the direction comes from the finer grid; `wdir`
at 0.5 degrees only). Data per 81-frame loop with Animation on (run 2026092518): wave height or period + direction about
20.5 MB on desktops at the default zoom, 32.8 MB zoomed in; 11.6 MB on phones at the default zoom, 24.0 MB zoomed in to
7.5 and 32.8 MB beyond; wind + wind direction 22.9 MB (46 MB past zoom 7.5). On phones the Opacity / Contours / Animation
row sits right under the timeline, and the sheet's header line leads with the forecast hour, then the valid time, and
shows a pending seek ("+213 h → +9 h loading…"). A direction is shown only for the step on the map: a step change clears the animation until that step's
direction frame has landed, so an older direction is never drawn under a newer time. The flow is built as vectors on
the model's nodes (the field value under the node times its FROM direction; the direction is the spectral peak's, NOAA
DIRPW: the partition carrying the peak period, which is not always the forecast table's first swell) and interpolated
between nodes as vectors, so a cyclone turns smoothly and slows to nothing at its eye; the particles read the flow
bilinearly and advance with a midpoint step. Particles run only where the field is drawn (data, not land: the tile land
masks, the readout's own rule; nothing under 0.1 m of wave height on the wave-height layer or a 3 s period on the period
layer, the model's no-wave floor along the ice margins). Speeds in screen px/s times the Mercator stretch (capped at 3):
wind 3 per m/s; wave height 8 + 3 per metre; period 1.5 per second (the group speed grows with the period). About one
particle per 900 screen pixels squared (150-3,000, fewer when a frame runs past 4 ms on desktops / 8 ms on phones), each
a bright head with a dim tail of its last ~0.6 s of positions, a dark halo under a light core so they read over the dark
ocean and the light desert or ice imagery alike, drawn fresh over a cleared canvas every frame (no compositing fade, so
nothing accumulates); wind particles live 1-2.5 s, swell particles 2-4.5 s. Under the viewer's reduced-motion setting
nothing animates and no direction frames are fetched (the checkbox is disabled and says so). The animation stops on Off, when
unticked, in a hidden tab, and while the map moves or zooms (rebuilt at the end). Runs published before the direction
fields existed disable the checkbox
("This run has no direction data").
Wind look (asset 2.11.1): under the wind overlay only, the page's satellite imagery gives way to Esri's World_Hillshade
relief map (a flat light sea, grey terrain). The wind colours (blue at the calm end, a hue change about every 2 m/s) are
multiplied into it, so the sea shows the colour itself and land the colour shaded by the terrain, and the GSHHG coastlines
are drawn into the wind tiles as thin dark lines (from the same per-tile land masks that clip wave height and period; the
wind is never clipped). The imagery is back for wave height, peak period, a field still loading and Off; the new basemap is
added on top and the old one removed once the new one's tiles have faded in (the blend is on only while the relief is the
sole basemap), and a relief that loads no tile is retried after 10 minutes. Wind opens at 90 % opacity, the waves at 65 %; each slider value is remembered for the tab (`opacityWind`,
`opacity`). The page hands its imagery layer to the module (`baseLayer`, found by its World_Imagery URL inside the gated
block); without it the basemap is left alone.
Coastline clip (asset 2.7.6): wave height and peak period are clipped to the ocean in the browser, so the field
stops exactly at the coastline; wind is never clipped. Every map tile's land alpha is rasterised once per tile per
zoom from GSHHG polygons served beside the frames under `static/coast/v1/` (tier 0 `world-i.bin`, ~1 km, for
tile zoom <= 6; tier 1 `f/<lat>_<lon>.bin`, full resolution, one 5-degree cell per file, fetched for the cells in
view at zoom >= 7, <= 32 MB of decoded chunks kept). The hover/long-press readout consults the same mask and shows
nothing over land. If the coast data cannot be loaded the field is drawn unclipped with a warning in the panel.
See tools/coast/README.md for the builder, the publish workflow and the LGPL notice. Review records: `docs/reviews/overlays-G*-adversarial.md`. Attribution on the map: "Overlay: NOAA GFS-Wave/GFS"; the panel carries the full
sentence ("Source: NOAA/NCEP GFS-Wave (WAVEWATCH III) and GFS via NOAA Open Data Dissemination; rendered by
Allshore Surf. Not an official NWS product.").
