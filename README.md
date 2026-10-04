# NOAA GFS Wave Table View Web App

This repository contains a Flask web application that fetches the latest NOAA GFS wave `.bull` file for any available buoy station, parses the data, and displays it in the same format as the **Table View** worksheet from an Excel file provided by the user. The output includes a metadata row, two header rows (parameter names and units), a blank row, and the data rows.

## Features

- **Buoy Dropdown**: The home page includes a dropdown menu preloaded with the NOAA GFS stations available in the `.bull` directory (see "The forecast-station list" below). You can select any buoy station to view the latest model data.
- **Automatic Run Detection**: The application automatically determines the most recent model run (18z, 12z, 06z, or 00z) by probing the NOAA directory structure for the selected date and hour. If the latest run isn't available, it falls back to earlier runs.
- **Excel-Style Table**: The data is formatted to mimic the two-level header structure found in the provided Excel **Table View** sheet, including a metadata row for cycle information and units row for each parameter. A blank separator row is also included for clarity.
- **Forecast points**: Map tools -> **Forecast point** gives the same table and graphs as a buoy station for any
  point on the open sea, titled with its coordinates, and keeps it under **My points** in the visitor's browser (see
  "Forecast points" below).
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
6. Under **Settings -> Health Checks**, set **Health Check Path** to `/healthz` (see "Live buoys: background refresh"
   below) — only once the deployed code has that route (a 404 counts as failing: Render would restart the service). A
   deploy then switches traffic when the new instance serves (and has the full live-buoy list, or 10 s have passed);
   visitors' pages show the buoys they remember in the meantime.

Once deployed, navigate to the provided URL to access the app. The site will allow you to choose from the list of available buoys and view the latest data.

## File Structure

- `app.py` — The main Flask application. It includes the routes for the home page and Excel download, the logic to detect the latest model run, fetch `.bull` files, parse them, and format the output.
- `requirements.txt` — Lists the Python dependencies needed to run the app (`Flask`, `pandas`, `requests`, `openpyxl`, `gunicorn`, `pytz`).
- `templates/index.html` — Jinja2 template containing the HTML structure for the home page: the map (the page), the Leaflet map and the forecast window (see below); Bootstrap for styling.
- `static_ui/forecast.js` — the forecast window's client module (see below); `static_ui/tools.js` — the map tools,
  including the Forecast point tool; `static_overlay/` — the optional model overlays.
- `point_forecast.py` — reads the forecast-point product for one ocean point: the water / land test, the model cell,
  the rows and the swell rank (see "Forecast points: the reader").
- `tools/model_frames/points.py` and `pointfmt.py` — the GitHub Actions job that publishes the forecast-point product,
  and its file format (shared with the reader).
- `buoy_sources.py` — the live-buoy providers (one per agency) and their background refresh; `static_ui/livelist.js` —
  the page's live-buoy list loader (see "Live buoys: background refresh").
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
model-overlay selector, the settings gear (Time Zone, Units, lat/long gridlines) top-right under the layer legend, the
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
`sessionStorage 'chartRange'` as before; the table's Detailed | Summary mode in `sessionStorage 'allshore.tableMode.v1'`
(plan section 35: the tab's own preference, never in the address). The table is the compact form (short dates with the
full date on hover, no info rows: the window shows Cycle / Location / Time Zone in one line above it). It fills its
window: a wider window spreads the columns (and past 1,500 px of body the text grows from 12.5 to 13.5 px, a CSS
container query), a narrower one scrolls the table sideways with the Date and Time columns staying put; the window's
width is the viewer's own in every view (the old cap at the table's width is gone). The
page is a shell on every JS load (the window fetches the forecast; the `#forecastLoading` placeholder stays until it
lands, which the overlay's restore waits for); `?render=full`, reached by the `<noscript>` meta refresh, renders the
forecast inline in the window's compact form and the module shows it without a fetch; without JS the window is a plain
card in the page flow with a Go button in the window's toolbar. The map fills the viewport (on phones less the
minimised bar) and re-applies its stored view only when the width changes (a phone keyboard must not snap it back). The
page golden (`tests/fixtures/index_golden.json`) was re-baselined at this restructure (see `tests/test_overlay_flag.py`).

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
(`u8-linear-v2`) -> Cloudflare R2. The frames cover the models' full output: hourly to +120 h, then every 3 h to +384 h
(209 frames; the manifest's `frame_schedule`). A run goes live only when all of it is on NOAA's bucket, about 40 minutes
after +240 h (27-47 minutes over five cycles measured 2026-09-26), so about 5.5-6 h after the cycle including the build;
runs published before this change (merged 2026-09-26) have 81 frames to +240 h, and the client plays any number of frames. Bucket layout `gfswave/0p25/v1/<RUN>/{half/}<field>/fNNN.png`,
`manifest-<ts>.json`, `stats-<ts>.json`, `latest.json` (written last, only for a complete run) and
`failed/<RUN>.json`. The 0.25 degree grid (~28 km) is sampled per map pixel; smoothing between grid points
is not extra detail. Fields: `hs` (HTSGW), `tp` (PERPW, the peak period), `wind` (10 m speed from GFS u/v), and
for the animation `pdir` (DIRPW, the peak / dominant wave direction) and `wdir` (10 m wind direction from u/v),
both in degrees true the waves / wind come FROM, coded circularly (`q = 1 + round(deg * 254 / 360) mod 254`,
the ordinary `u8-linear-v2` decode over 0-360, error <= 0.71 deg; the manifest marks them `circular`,
`convention: from`, `interpolation: circular`). `pdir` is published full + half and takes the coastal fill
by nearest model cell (never a mean of angles); `wdir` only at half resolution (`resolutions` per field in the
manifest). The job records per step how many cells have at least 0.1 m of waves but no direction, or a direction
without a height (`pdir_mask_mismatch` in the stats sidecar, on the model grids; a calm under 0.1 m, where no
particle is drawn, is counted apart as `pdir_missing_calm`) and warns in the summary. The build is a pipeline with
unchanged output bytes: the next step's records download while a step is decoded and encoded, the fields encode in
parallel, the PNGs upload in a pool behind the build (every frame is stored before the manifest and the pointer), and
GRIB decoding stays on one thread (eccodes is not thread-safe). For `pdir`/`wdir` the `encoding_spec` clamp does not apply: code 1 is north (0 = 360 degrees) and code
255 is never used; a reader must branch on the field's `circular` flag.

Runbook: rotate the R2 token in the repository secrets; roll back by unsetting `MODEL_OVERLAYS` (env only,
no deploy), by re-pushing the `prod-pre-overlays` tag, or by disabling BOTH workflows (`model-frames.yml`
and `model-frames-keepalive.yml`, which would otherwise re-enable the job on the 1st and 15th; the last
complete run stays live and the panel shows a stale banner after 9 h). Integrate site changes into
`Live-Buoy-Update` by MERGE, never by pushing a feature branch over it: the job-only merges live on the
production branch alone. The workflow step summary states how long after the cycle each publish happened;
`failed/<RUN>.json` and `notready/<RUN>.json` in the bucket record build failures and listed-but-missing
objects (error text is redacted: no endpoint URL, account id or bucket name reaches the public bucket).
The frames are public only through the bucket's custom domain (`models.allshoresurf.com`, Cloudflare-proxied,
edge-cached, nosniff + sandbox CSP); the r2.dev development URL is disabled -- re-enable it in the bucket's
Settings and point `MODEL_FRAMES_BASE` at it only as an emergency fallback. `--force` re-uploads a run under
its EXISTING immutable keys: the edge cache and browsers keep the old bytes for up to a year, so use it only
to re-send identical bytes (e.g. after an interrupted upload); any change to the encoding or layout goes under
a new `PREFIX` version instead. A change of a field's encode RANGE (lo/hi in `encode.FIELDS`) needs neither: the
client decodes each run with its own manifest lo/hi, so old and new runs coexist -- but never `--force` a run
published under the old range (plan section 27: hs 0-75 ft with a 0-60 ft legend, wind to 120 kt). The publisher pins its conda packages and action commits and lists the
installed versions in the step summary; bump the pins deliberately. External cron trigger: GitHub drops or delays
most `schedule` ticks, so a Cloudflare Worker (`tools/model_frames/trigger/`, deployed by Cloudflare Workers Builds
from this repository -- root directory `tools/model_frames/trigger`, build command `npm run build`, deploy command
`npx wrangler deploy`) calls the workflow-dispatch API at :05, :15, ... UTC for every workflow in its `GH_WORKFLOW`
list (the frames and the forecast-point product below); GitHub's own schedule stays on as a
fallback and a duplicate run short-circuits. The Worker needs the secret `GITHUB_TOKEN`, a fine-grained token for
this repository with "Actions: read and write", entered under the Worker's Settings > Variables and Secrets (rotate
it there; it is never in the repository). Runs it starts show the event `workflow_dispatch` with the token's owner
as actor. To stop it, disable the cron under the Worker's Settings > Triggers (or delete the Worker); a full stop
also disables both workflows as above. Data policy in the browser: the 0.5 degree frames are used below zoom 3.5 on desktops, below zoom 7
on narrow (phone) maps and for wind everywhere, so a full 209-frame loop of the field frames is about 10-34 MB (83 MB only
for wind zoomed past 7.5); with the particle animation (always on since asset 2.12.0) the direction frames add to that (see
the Animation paragraph: about 53 / 85 MB on desktops, 30-62 MB on phones, 59 / 119 MB for wind). The FIRST picture of a
field never waits for its direction frame (the particles join when it lands); later steps change picture and particles
together. Frames are immutable, so a second loop costs nothing. Frames are decoded without a canvas
where the browser has DecompressionStream.
Overlay state (asset 2.7.6): the chosen layer, its valid time and whether it was playing are kept in the tab's
sessionStorage (`allshore.overlay.v1`, with the playback speed), so picking another forecast
point (a page reload) brings the overlay back where it was. The time and play state come only from a save under 30
minutes old, counted from when the page was left (hiding or leaving the page refreshes the save, paused or not), never
under the viewer's reduced-motion setting, and a hidden tab resumes when shown. The restore starts after the load
event (5 s at most), a deferred forecast table (3 s at most) and an idle moment, with "Loading" in the panel
meanwhile. Off, a new tab, or a layer picked from Off starts without a saved time; switching layers while the saved
one waits to be restored keeps the time; a page shown again from the back/forward cache writes its own state back to
the tab.
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
Animation (asset 2.9.5; always on since asset 2.12.0, no checkbox): the overlay draws
particles with fading trails that flow along the dominant swell direction under wave height and peak period, and along
the wind under wind speed, on a canvas of their own under the forecast points (no pointer events; markers keep their
taps). The directions come from the `pdir` / `wdir` frames of the same step, loaded through the frame scheduler
beside the field frame (the target's field frame first, then its direction, then the ring of frames around it, never more
than two downloads at once; `pdir` at the 0.5 degree frames at the default zoom 6 and wider and the 0.25 degree ones as
soon as the map is zoomed in, from 6.5 (back below 6.25), so near coasts the direction comes from the finer grid; `wdir`
at 0.5 degrees only). Data per full 209-frame loop with Animation on (scaled from run 2026092518): wave height or period
+ direction about 53 MB on desktops at the default zoom, 85 MB zoomed in; 30 MB on phones at the default zoom, 62 MB zoomed
in to 7.5 and 85 MB beyond; wind + wind direction 59 MB (119 MB past zoom 7.5). A 1x loop takes about 105 s (the hourly
first five days at 2 h/s, then 6 h/s). The timeline is laid out by time (+120 h at 31 % of the track) and snaps to a frame
in the direction of travel. The panel's run line says when the run went live and when the next is expected ("live since
1:07 PM HST · next update about 7:10 PM HST": one cycle after this run's own publish time), in the computer's own time
zone (asset 2.10.1). On phones the sheet's header line leads with the forecast hour, then the valid time, and
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
nothing animates and no direction frames are fetched. The animation stops on Off, in a hidden tab, and while the map moves
or zooms (rebuilt at the end). Runs published before the direction fields existed show no particles.
Wind look (asset 2.11.1): under the wind overlay only, the page's satellite imagery gives way to Esri's World_Hillshade
relief map (a flat light sea, grey terrain). The wind colours (blue at the calm end, a hue change about every 2 m/s) are
multiplied into it, so the sea shows the colour itself and land the colour shaded by the terrain, and the GSHHG coastlines
are drawn into the wind tiles as thin dark lines (from the same per-tile land masks that clip wave height and period; the
wind is never clipped). The imagery is back for wave height, peak period, a field still loading and Off; the new basemap is
added on top and the old one removed once the new one's tiles have faded in (the blend is on only while the relief is the
sole basemap), and a relief that loads no tile is retried after 10 minutes. Fixed opacity since asset 2.12.0 (no slider; `FIXED_OPACITY`): wind 100 %, wave height and period 65 %. The page hands its imagery layer to the module (`baseLayer`, found by its World_Imagery URL inside the gated
block); without it the basemap is left alone.
Simpler panel (asset 2.12.2, owner 2026-09-26): no Opacity / Contours / Animation row (on phones the legend sits right
under the timeline), and the page's +/- zoom buttons are
removed by the gated overlay block (`map.removeControl(map.zoomControl)`; the wheel, pinch, double-click and keyboard still
zoom, the Home button stays); with the overlay flag unset the page is unchanged.
Coastline clip (asset 2.7.6): wave height and peak period are clipped to the ocean in the browser, so the field
stops exactly at the coastline; wind is never clipped. Every map tile's land alpha is rasterised once per tile per
zoom from GSHHG polygons served beside the frames under `static/coast/v1/` (tier 0 `world-i.bin`, ~1 km, for
tile zoom <= 6; tier 1 `f/<lat>_<lon>.bin`, full resolution, one 5-degree cell per file, fetched for the cells in
view at zoom >= 7, <= 32 MB of decoded chunks kept). The hover/long-press readout consults the same mask and shows
nothing over land. If the coast data cannot be loaded the field is drawn unclipped with a warning in the panel.
Coastal fill (job): GFS-Wave leaves cells with enough land in them empty, so before quantisation the job fills empty
wave-height and peak-period cells within 4 cells (1 degree) of model data, and only within one cell of GSHHG land
(`tools/model_frames/fill_allow.png`, built from the published coast data by `make_fill_mask.py` and pinned by hash),
so open water and the sea-ice pack the model masks are never filled (along ice-bound coasts the fill can still
reach up to one cell, ~28 km, over coastal sea ice). Wave height takes the mean of its present
neighbours; peak period takes the value of the nearest model cell (a filled node is never a blend of two swell
regimes; runs published after 2026-09-25 declare peak period `bilinear`, so the browser draws it smoothly like
wave height; older runs keep `nearest` until they age out of retention, about a day). Model values
are never changed; the manifest carries a `fill` block and the stats sidecar `filled_points`; the browser's coastline
clip hides the fill over land. Bays more than 1 degree from model water stay empty. A run published with a different
fill is never rebuilt (`run.py` refuses, even with `--force`; frame keys are immutable), and a complete run whose
pointer write failed is re-pointed instead. Rollback: set `"fill": False` for hs, tp and pdir in `encode.FIELDS` and adjust
the fill tests in the same commit (keeps the guard); the filled live run stays live until the next cycle (up to ~6 h)
unless `latest.json` is pointed at an older unfilled run by hand -- never `git revert` the fill commit, which would
drop the guard as well.
See tools/coast/README.md for the builder, the publish workflow and the LGPL notice. Review records: `docs/reviews/overlays-G*-adversarial.md`. Attribution on the map: "Overlay: NOAA GFS-Wave/GFS"; the panel carries the full
sentence ("Source: NOAA/NCEP GFS-Wave (WAVEWATCH III) and GFS via NOAA Open Data Dissemination; rendered by
Allshore Surf. Not an official NWS product.").

## Forecast points

Live on allshoresurf.com since 2026-10-03 (UI asset 1.15.2; rollback tag `prod-pre-point`). A visitor picks **Map tools
-> Forecast point** and clicks (or taps) the sea. The server checks the point first:

- **Open water**: the forecast window opens with the same table and graphs a buoy station has, titled with the
  point's coordinates ("21.700N 158.200W"), and the point is kept under **My points** (a ring on the map, an entry at
  the top of the station picker; rename and remove there). Where the point looks out to the open sea through a narrow
  mouth, the Location line adds the distance, e.g. "37.811N 122.477W (open water 31 km away)".
- **Refused**, with the reason in the tool bar and the tool left on for another click: land (including the first few
  hundred metres where the coastline data is off the real shore: "try a point a little farther from the shore");
  sheltered water whose nearest model points lie beyond land (San Francisco Bay, Pearl Harbor, Moreton Bay); water the
  model does not cover (sea ice, the Black Sea).
- **How it differs from a station**: the values are those of the nearest NOAA GFS-Wave model cell reachable over water
  (1/6 degree, about 18 km; 1/4 degree south of 12.75 S and north of 52.25 N), with the wind sea and up to three
  swells (bulletins have up to six); rows are hourly to +120 h, then every 3 hours to +384 h; a run appears about 6 h
  after its cycle. The time zone is the nearest station's (across the date line, the point's own).
- **Swell order, site-wide**: in every table (points, NOAA stations, SWAN, the live-buoy components) each hour's
  systems are ranked by height squared x peak period, so "Swell 1" is the system that makes the most surf. The
  numbers in a station's row are NOAA's; only their columns follow this rank.
- **Data path**: GitHub Actions (`model-points.yml`) -> the R2 bucket (`gfswave/points/v1`) -> `point_forecast.py` on
  the server -> `/api/forecast?station=pt_...` -> the page. No third-party API; $0 while the
  repo is public (Actions minutes) and the bucket stays in R2's free tier.
- **Reviews**: `docs/reviews/forecast-point-G22a-adversarial.md` (the job) and
  `docs/reviews/forecast-point-G22-adversarial.md` (the reader and the page: the reviews, the owner's decisions, three
  fix rounds and the test-site verifications).

The three sections below describe the job, the reader and the page in detail.

## The forecast-station list (plan section 32)

`station_list.json` (the station dropdown, the picker and the map's forecast markers via `/stations.json`) holds 734
points: 461 numbered buoys and 273 named points (more buoys and platforms such as the BSH, LF and TF sets, the forecast
offices' own points numbered below 51 such as HNL01 or MIA01, the OPC and TPC offshore points, the Pacific islands, the
African and Indian Ocean coastal points, and others). Since 2026-10-03 (owner) it leaves out the 3,302 BOUNDARY points
of regional and coastal models, which drew boxes around ocean areas on the map: the NWPS edges of each NWS office
(`NW-<office><n>`), the hurricane model's domain edges (`HWRF<basin>-<n>`), the NHC domain edges `RW-NH1`/`RW-NH2`,
other models' edges (`BKMG` Indonesia, `CDIP` Scripps' Southern California model, `KNY`, `MDG`, `SYC`) and the older
office sets numbered 51 and up (KEY, MIA, SJU, BER, MLB, PCB, SRH, CRP, LIX, TBW, JXFL, LCH, CHS, HGX, CCTX, MOB, BRO,
JAX, MNE, HNL; HNL51-68 are the boxes around Oahu and Kauai). `tests/test_station_list.py` holds the rule.
- Source of the rule: NOAA's own point list for GFS-Wave (`parm/wave/wave_gfs.buoys.full` in NOAA-EMC/global-workflow)
  gives each point a TYPE. Every removed id is a boundary type there (3,118 `IBP` for NCEP models, 184 `BPT` for other
  models); the kept ones are data points (`DAT`, `XDT`) and virtual buoys (`VBY`), with one exception kept on purpose:
  `DIABLO_01` (`BPT`, a single point that draws no box). If NOAA's list changes, re-derive the rule from that column.
- `station_coords.json` and `station_timezones.json` keep all 4,036 entries: they are lookup tables, and the
  forecast-point time-zone rule (below) reads every coordinate. Taking the boundary points out there would move point
  time zones (measured: 202 of 1,078 sample points, some for the worse).
- A link to a removed point (`?station=NW-HFO51`) still works: its forecast is NOAA's bulletin, shown under its raw id
  like any id not in the list.

## Forecast-point product (job; plan section 31)

`tools/model_frames/points.py` (GitHub Actions `model-points.yml`: its own workflow and concurrency group, dispatched
every 10 minutes by the same Cloudflare Worker as the frames, GitHub's schedule as the fallback) publishes, for every
sea cell of NOAA's gridded GFS-Wave output and every step of the newest COMPLETE run, what a point bulletin holds:
the combined wave height, the wind sea and three swell partitions (height, PEAK period, direction the waves come
FROM) and the wind (speed, direction it blows FROM). The site reads one tile to build a forecast table for any
ocean point (`point_forecast.py`, next section; this section is the job).

- **Grids** (`pointfmt.GRIDS`; the rows that hold waves do not overlap and leave no gap): `g16` = `gfswave
  global.0p16` (1/6 degree, 52.167 N to 12.5 S, the model's own grid), `s25` = `gfswave gsouth.0p25` (1/4 degree,
  12.75 S to 79.5 S, the model's own grid), `n25` = `gfswave global.0p25` (1/4 degree, 52.25 N northward; NOAA's
  interpolated global grid, the only one that reaches there). NOAA's `global.0p16` FILE spans 52.5 N to 15 S, but
  its first two and last fifteen rows carry no waves (most carry wind), so the other grids are stored up to the rows
  it has waves for (cutting at the file's bounds left no wave data from 12.75 S to 14.85 S: review G22a). Every build
  checks that those rows are still the ones `pointfmt.GRIDS` expects (`data`) and fails if NOAA moved them. A cell
  is a grid point: latitude `lat0 - row / per_deg`, longitude `col / per_deg`. A reader takes the nearest sea cell
  of any grid; on a tie, the earlier grid. One pocket is left INSIDE g16's band: off the Dutch coast (51.5-52.0 N,
  3.5-3.75 E) NOAA's `global.0p16` has no waves on a few cells, so four cells of water are more than 1.5 cells from
  any stored cell (the same in every cycle checked); the reader's 40 km reach covers it.
- **Records**: 15 per file, found by their GRIB2 code numbers (discipline, category, number, surface type, and the
  sequence number 1-3 of a swell partition), never by a decoder's short names; every used record is checked for its
  cycle, forecast hour, grid geometry and a plausible range, and a file with a missing or duplicated record fails
  the build. NOAA orders the three swell partitions by height at every step (on the interpolated `n25` grid not
  always, and there the wind sea can even exceed the combined height): partition 1 at one step is not the same
  swell train as partition 1 at the next, and a reader must not rely on the order (the site ranks every row itself).
- **Values**: uint16 at the bulletins' precision: heights 0.01 m, periods 0.1 s, directions 1 degree (360 stored as
  0), wind 0.1 m/s. NOAA's files hold two decimals, so ties are common; they all round up. Land and ice cells are
  not stored at all. 65535 inside a tile means no value at that step: a partition the model did not find, or a sea
  cell without waves at that step.
- **Layout** (`gfswave/points/v1`, its own prefix: the frames' pruning never sees it and this job's never sees the
  frames): `<RUN>/<grid>/<tr>_<tc>.bin` (one 4-degree tile: a JSON header, the block's sea bitmap, then one xz
  stream of the planes `[field, step, cell]`), `<RUN>/<grid>/mask.bin` (the grid's sea mask), `stats-<ts>.json`,
  `manifest-<ts>.json`, then `latest.json` LAST and only for a complete run; `failed/<RUN>.json` and
  `notready/<RUN>.json` as for the frames. About 2,900 objects and 1.0 GB a run; two runs are kept. Format `apt1` is
  defined, byte by byte, in `tools/model_frames/pointfmt.py` (pure numpy + standard library: the reader uses the
  same module). Its decoders accept only what a real tile can be (at most 16 MB unpacked, xz only, the dictionary
  capped) and raise `ValueError` for anything else.
- **Sea cells**: the cells with a wave height at the build's first step, inside the grid's stored rows. NOAA's land
  and ice mask is the same at every step of a run. A later step that disagrees is counted: a cell that lost its
  waves is stored as missing there, one that gained waves is not in the product; the manifest's `sea_cells` says how
  many, the step summary warns, and beyond 1 % of a grid's cells at any step the build fails instead of publishing.
- **How a build runs**: each step's three files are downloaded whole (about 28 MB a step, 6 GB a run, anonymous S3;
  a failed or damaged download is tried three times) and decoded in worker processes (eccodes is not thread-safe);
  each worker writes its step into a scratch file on the runner's disk (about 5.6 GB: a whole run does not fit in
  memory); the first failure stops the build when it happens (a file missing late in the run is still only asked for
  after the steps before it: about 3 minutes and 3 GB of downloads, again on every tick until NOAA serves it). The tiles are then cut one tile row at a time, compressed in
  threads and uploaded behind the build. Every tile and mask is stored before the manifest, the manifest before the
  pointer. About 22 minutes on the runner; the manifest carries a `digest` of every stored value (two builds of the
  same NOAA files agree on it exactly when they stored the same values).
- **Guards**: a run that is live is left alone; without `--force` the pointer never moves back, also not when a
  newer run went live while this one was building; a failed build is retried after 3 h, three times at most; a
  complete run whose pointer write failed is re-pointed on the next tick, not rebuilt; a run published in another
  format (`format_key` in the manifest: fields, scales, grids, tile size, step schedule) is never rewritten, not even
  with `--force` -- a format change takes a new prefix version. `--force` rebuilds the newest complete run even when
  a newer one is live and then points to it: use it only knowing that.
- **Run it**: `python tools/model_frames/points.py` (R2 secrets in the environment), `--dry-run` (build everything,
  upload nothing), `--local DIR` (publish into a directory, same layout), `--steps 0,3,6` (a subset) and `--run
  YYYYMMDDHH` (a given cycle): both only with `--dry-run` or `--local`, a subset is never pointed to; `--force`,
  `--keep N` (default 2), `--workers N` (0 = in this process). The workflow's `steps` input always implies a dry run.
- **Stop it**: disable `model-points.yml` (and remove it from `model-frames-keepalive.yml`, which re-enables it twice a
  month); the Worker's tick then reports an error for that workflow and still dispatches the frames. The last complete
  run stays in the bucket.
- **NOAA changes on the horizon**: GFS v17 (proposed for late 2026) renames wave files and may change grids. A change
  of grid geometry, record identity or the rows with waves fails the build loudly, with a failure record. RENAMED
  files do not: the run then never counts as complete, every tick ends quietly as "no complete run", and the only
  signal is the age of the run `latest.json` names (the reader must show it). The file tags, grid geometry, record
  identities and step schedule are constants in `pointfmt.py` / `fetch.py`.

## Forecast points: the reader (plan section 31, step 4)

`/api/forecast?station=pt_21667N_158054W` answers for ANY ocean point with the payload a station gets: the same
table and graph data, built by the same code, from the forecast-point product above. `point_forecast.py` does the
reading; `app.py` (`point_forecast_data`) puts the service's limits around it.

- **The id**: `pt_<latitude in thousandths><N|S>_<longitude in thousandths><E|W>`, one spelling per point (no leading
  zeros, zero is N / E, the antimeridian is 180 W); anything else is "Invalid forecast point". No NOAA station id
  starts with `pt_`.
- **Water or land** (owner, 2026-10-02: land is refused): the site's own coastlines (`static/coast/v1`, the
  full-resolution GSHHG cells the swell-exposure tool reads) decide, at the id's coordinates, on the server, for tool
  clicks, links and saved points alike. Water = the coast data says water, or water lies within 300 m (`SHORE_M`,
  searched on rings every 50 m in 32 directions: a beach, a pier, the data's own error; the id of Pipeline's line-up
  is "land" by 70 m). Land: "That point is land or inland water in the site's coastline data. If it is the sea, try
  a point a little farther from the shore." (`reason: "land"`; owner, 2026-10-02: at a few breaks the data lies 1-2 km
  off the real shore - Puerto Escondido, the Peahi cliffs, inner Honolua Bay - and the band stays 300 m). The Python
  decoder and land test give the page's answers (`decodeCoastLL` / `inLand`) everywhere off the coastline itself;
  `tests/fixtures/coast/land_parity.json` is checked on both sides. A coast cell must lie inside its own 5-degree box,
  and a cell whose size disagrees with the index makes the index be read again. Coast data that cannot be read answers
  "temporarily unavailable": a point is never served untested.
- **The cell**: the nearest sea cell of any grid within 40 km (`REACH_KM`; on a tie the earlier grid) that can be
  REACHED OVER WATER (owner: water whose only cells lie beyond land is refused). The straight path from the point
  (from the nearest water when the point stands on the shore band; if no cell is reachable from there, from the band's
  other water samples, nearest first, at most 8 at least 100 m apart: a shore whose nearest water is a pocket) to the
  cell's centre is judged by its EXACT
  crossings of the coast edges (`path_crossings`, `land_runs`; longitudes unwrapped from the point, the cells beyond
  180 shifted by 360): any stretch of land of 100 m or more blocks it (`PATH_LAND_KM`), wherever it lies; only the
  stretch that reaches the cell's centre is forgiven, up to 3 km (`PATH_CENTRE_KM`: some sea cells have their centre
  on an islet or a headland; half a cell let a whole barrier island through, G22 R2-1: Moreton Bay). The nearest
  cell blocked: the next one; none: "No forecast here: the wave model's nearest points lie beyond land"
  (`reason: "sheltered"`: San Francisco Bay, Pearl Harbor, the Solent, Venice, lagoons behind barrier islands), or
  `reason: "land"` when the click itself is on the shore band's land. Water that sees an open-sea cell
  through a mouth is served from it (owner, 2026-10-02: the Golden Gate, Cowes, lower Tampa Bay). No sea cell within
  reach (sea ice; a sea the model does not have, such as the Black Sea, which is water in the coast data):
  `reason: "nodata"`; the Great Lakes are land in the coast data (`reason: "land"`). Measured on the reviewers'
  sets: 19 of 19 sheltered waters refused, 35 of 36 surf spots served (inner Honolua Bay: the coast data barely has
  the bay), 551 of the 596 live-buoy positions served. The table's and the graphs' Location line shows the clicked
  point's coordinates (owner, 2026-10-02: "Location : 21.700N 158.200W"); where the point is served through a narrow
  mouth, the open water's distance follows (owner, 2026-10-03: "37.811N 122.477W (open water 31 km away)"). A narrow
  mouth (`mouth_aperture`): along the served path, outside the model cell's own box, land within 10 km on BOTH sides
  (`MOUTH_CAP_KM`) makes a gap, and the narrowest gap, as seen from the point (atan(L/s) + atan(R/s)), is under 70
  degrees (`MOUTH_DEG`): Cowes 5, northern Pamlico Sound 23, lower Tampa Bay 40, Fort Point at the Golden Gate 67
  get the note; Hanalei Bay 85, Hilo Bay 112, coves such as Waimea and the open coast do not (about 3 % of the
  near-coast points served). Land on a ray is the first stretch of 100 m or more (the coast cells' closing edge pairs
  along the 5-degree lines are no land). Known misses: an entrance inside the cell's own box gets no note (Botany Bay,
  Western Port, Kaipara Harbour), and "open water" is the owner's word also where the cell lies in an archipelago.
  The payload's `point` carries `mouth_deg` (None: open water all the way) and says where
  the point and its cell are (`cell_lat`, `cell_lon`, `cell_km`, `grid`) and which run this is (`run`, `run_utc`,
  `published_utc`, `age_hours`); a refusal carries `final: true` and its `reason` (also `invalid`, and `off` without a bucket).
- **The rank of a row's swells** (owner, 2026-10-02, ONE rule for every forecast table of the site): at each hour the
  wind sea and the swells in descending order of height squared x peak period (`rank_groups`), packed from the left.
  That is the energy arriving per metre of crest in deep water and the only wave quantity in the usual breaker-height
  formula: "Swell 1" is the system that makes the most surf. NOAA's bulletins list their systems by height (100 % of
  53,577 rows of 225 bulletins); the site re-ranks them (`rank_rows`, in `parse_bull` and `parse_swan`: the numbers
  of a row stay, the columns they sit in change in about a third of rows), and the live-buoy components take the
  same order, so a point reads like the station beside it. A swell moves across columns as it grows and fades.
  Differences from a station's bulletin: four systems at most (the bulletins have up to six), rows hourly to
  +120 h and then every 3 hours, and the values are the model CELL's. North of 52.25 N the product comes from
  NOAA's interpolated quarter-degree grid: its systems are blended (the bulletin has the site's system in 93.5 % of
  cases against 98-99 % on the native grids).
- **The graphs' time axis**: `graph_data` of a point has one slot per HOUR of the run (385 labels, empty between the
  3-hourly rows), so a day is as wide on day 10 as on day 1, as a station's; the table keeps its 209 rows.
- **The time zone** (owner: the nearest station's): the civil zone of the point's own waters when the lookup gives
  one; else the zone of the nearest forecast station within 1,000 km (all 4,036 NOAA points, boundary points
  included: `station_coords.json`) unless it lies across the date line (12 hours or
  more from the point's nautical offset in January and in July; `_point_tz`: 340 km north of Oahu is Hawaii time,
  the Bering Sea keeps Nome time, but off the Commander Islands Asia/Kamchatka, not America/Adak); else the nearest
  land's; else the nautical
  zone, shown as "UTC-11", never "Etc/GMT+11" (`zone_label`, also on the no-script page).
- **Limits** (one worker, four threads): a cache of its own (`_POINT_CACHE`, 64 forecasts keyed by run, point and
  zone: points never push a station's forecast out); two point builds at a time, a third waits two seconds and is
  then told "The server is busy ... try again" (`busy: true`, not kept): one deadline for the whole wait, and at once
  when both builds have been stuck longer than that; a point's objects are fetched with ONE attempt (3 s to connect,
  6 s between bytes, about 8 s for the whole object from the request on: the socket of a body that trickles is shut
  down, which also stops a body with a Content-Length - closing the response from another thread waited for all of it;
  headers that trickle are not cut, R2 sends them at once); a tile that
  fails its check is not kept; the pointer is read every five minutes and, when it cannot be read, the last manifest serves for
  up to six hours; a 24 MB cap on kept tiles, 256 kept cell series (a change of time zone or units costs no fetch).
  `pointfmt` loads on the first point (numpy is already there: the time-zone lookup behind the live-buoy list loads
  it): about +22 MB then, +35 MB after sixteen points around the world. Measured against the live bucket: the first point 1.4 s, a new point about 0.5 s, a kept one 11 ms.
- **Where it reads**: env `POINTS_ROOT` (the bucket's public root), else `MODEL_FRAMES_BASE` without its
  `/gfswave/0p25/v1`; neither: "Forecast points are not available on this server". Object keys are built here, never
  taken from the manifest; every object is checked against what was asked for (run, grid, tile, steps, fields).
- **Check by hand**: `python tools/model_frames/check_points.py` compares the live product, read as the site reads
  it, with NOAA's bulletins of the same run at 45 stations (2026-10-02, run 2026100206: combined height within
  0.007 m at the median open-ocean station; 87 % of the bulletins' partitions of 0.3 m or more found, the rest being
  their fifth and sixth; the found ones within 0.03 m, 0.05 s and 1.4 degrees). Run it again when NOAA changes its
  wave products.

## Forecast points: the page (plan section 31, steps 5 and 8; UI asset 1.15.2)

- **The tool**: Map tools -> **Forecast point** (first in the menu). A click (or tap) ASKS the server first
  ("Checking that point..." in the tool bar; `AllshoreForecast.prefetch` fetches and keeps the forecast without
  showing it). A forecast: the tool ends and the point opens in the forecast window from that cache (the same table
  and graphs as a station, titled with its coordinates: "21.700N 158.200W", "Pipeline — 21.667N 158.054W" once named).
  A refusal (owner, 2026-10-02: land is refused; so is sheltered water and water without model data) or a failure:
  the server's message in the tool bar, the tool stays on for another click, nothing on screen changes, nothing is
  kept. A closed or restarted tool drops a late answer, and so does a visitor who opened another forecast meanwhile.
  An HTTP error is told as the server's, a failed connection as the connection's. The tool shows only where a points
  bucket is set.
- **My points**: kept in each visitor's own browser (localStorage `allshore.points.v1`, `[{id, lat, lon, name}]`, 50
  at most; the coordinates are read from the id, never trusted from storage; accounts may adopt the record later). A
  point opened with the tool is kept at once; when it cannot be (50 already, storage full or switched off) the window
  says so (`#fwNote`) and the screen-reader line too. A point opened from a link (`?station=pt_...`) is shown with a
  dashed ring and not kept until its star is set; the star cannot keep a refused point. Kept points are white rings
  with a pink edge (their own legend entry, "My points", ticked again when a point is added), options of the station
  select (an optgroup the page adds; the no-script page renders the point's own option), and a "My points" group at
  the top of the picker's list with Rename (the browser's prompt) and Remove (a named point asks first). The arrow
  keys, Home and End move between the list's rows. Another tab's changes arrive through the `storage` event.
- **Names**: cleaned (no control or bidi-control characters), 40 characters as the visitor counts them, isolated
  (U+2068 / U+2069) so a right-to-left name cannot turn the coordinates round; written as text everywhere,
  tooltips included (a Leaflet tooltip given a string is HTML: the point and live-buoy tooltips are text nodes).
  Where room runs out the NAME is cut, never the coordinates (`writeLabel`, CSS `.lbl-split`); on phones the run
  text yields first.
- **The window**: the run's age is shown after the cycle once it passes 13 hours ("· 15 h old": a NOAA cycle was
  missed). A point's graph data has one slot an hour (the server fills the 3-hourly part with gaps), so days are as
  wide as a station's and the 7-day and 3-day ranges count rows as for every station; the tooltip and the
  three-chart sync take the nearest ROW (a Chart.js interaction mode of the page's own, `allshoreRow`: Chart.js's
  own modes went blank between the 3-hourly rows). The row is the nearest one with a value in a SHOWN series (a
  series hidden from the legend does not count), and the other two charts list only the swells present at that row
  (no "Swell 4: 0"); stations keep Chart.js's index mode with the same sync rule.
  A refusal or error is said once, in the window's box; a failed load clears the last table, the run text, the
  model bar and the note; Retry keeps the keyboard in the window. A refusal is forgotten when a forecast for the
  point lands. Nautical zones are written as offsets ("Etc/GMT+11" ->
  "UTC-11"). A map tool's click on a point marker goes to the tool. Names are cut by what a visitor sees as one
  character (a family emoji or a flag is one).
- **Ids**: `static_ui/forecast.js` `pointId` follows `point_forecast.point_id` exactly; `tests/fixtures/point_ids.json`
  (522 inputs) is checked against both.

## The sky in the forecast table and graphs (plan section 35, UI asset 1.16.8)

The window's table and graphs know the real sun and moon at the station (`sky.py`, PyEphem; `requirements.txt`
`ephem==4.2.1`; checked to the minute against USNO in `tests/test_sky.py`). What the server sends (`/api/forecast?compact=1`
only; the classic table and payload are unchanged apart from the relabel; without coordinates, without ephem or on any
error the table keeps its old 6 AM - 7 PM bold rule and the graphs their fixed night hours):

- `table_html`: rows classed `sky-day` when any part of the row's FIRST HOUR lies between first light and last light
  (the sun's centre above -6° at the row's time, or first light inside that hour: with first light at 6:02 the 6 AM row
  is a daylight row; owner: a row at least partly in daylight looks like full daylight, there is no twilight look, and a
  3-hourly row is judged by its first hour, like the graphs' hourly shading), else `sky-night`; events are slotted by
  the minute they are shown at; `day-first` on each day's first row, `now-row` on the current hour, `data-t`. Without sky data the window's
  rows get the same classes from the fixed 6 AM - 7 PM clock rule (the classic table keeps that rule inline). Columns: Date, Time, Sig. Wave Height, the swells, Wind, Sun/Moon (owner: the
  significant height first after Time, Sun/Moon last). "Comb." is now "Sig. Wave Height" (the bulletins' Hst, SWAN's
  Hsig, a point's HTSGW: the significant wave height of the combined seas; the classic table keeps it after the swells,
  named "Significant Wave Height"). The Sun/Moon column (`col-sun`): first light ◐, sunrise ☀↑, sunset ☀↓, last light ◑,
  moonrise ☾↑, moonset ☾↓ in the row's slot (an hourly row's hour or a 3-hourly row's three hours; "AM" / "PM" added
  when the event's half of the day differs from the row's, e.g. a 12:40 AM moonrise in a 3-hourly 11 PM row), the
  moon's phase glyph and lit fraction on the first row of each night (its name in the hover text: a principal phase
  only within 12 h of its instant, else waxing / waning crescent / gibbous, on USNO's phase instants; USNO's own daily name can differ by a day). The whole table is ASCII:
  the glyphs are numeric character references (one non-ASCII character in the `html +=` string made a 385-row build
  ~30x slower); the rows' parsed times and the new moons are cached, so a window payload costs ~17 ms against the
  classic one's ~8 ms (it was ~290 ms). The right-edge cells carry `col-last`. `col-date` / `col-time` on the first two cells (the sticky
  columns); directions read "248° WSW" with an arrow (`.dir-arrow`, rotated to where the waves go).
- `summary_html`: the Summary view, one row per forecast day over its daylight rows (first light to last light):
  significant wave height range + trend (↗ rising / ↘ falling / ▲ peak at an hour / → steady: the last third of the window
  against the first, 10 % bands), the two most powerful swell SYSTEMS of the day (the hourly columns are re-ranked by
  power, so a column is not a system: every hour's samples are grouped by period, within 20 % and at least 1.5 s, and
  direction, within 40°; power = height² × period), wind range + mean direction, sunrise, sunset, the moon (tonight's:
  the first badge from the date's noon to the next noon, else the moon at tonight's last light). The day's samples are
  the rows whose own time lies between first light and last light (a row only partly in daylight looks like daylight but
  its value is taken outside it). A first or last day without such samples is left out; a polar night keeps a row with a
  note; only the forecast's first and last dates can be cut: "from 8:00 AM" (its first row more than an hour after
  sunrise, else first light, else midnight under the midnight sun) and / or "until 2:00 PM" (likewise before sunset).
- `graph_data.sky`: one state per slot (a point's hourly slots included), for the charts' shading.

The page: daylight rows are bold with solid black borders and night rows normal with dashed grey ones (the look the
clock rule always had; the Date column is bold only in daylight rows, like every other cell), a `--tint` gradient laid over
each cell's own swell colour (night slate-blue, hover, the now row blue with a left accent), a 2 px rule where each day
starts, tabular numerals, the group headers softened 15 %, Date and Time sticky on the left (the Time column's offset is the
Date column's measured width, `--date-w`, re-measured whenever the detailed table shows), the Sun/Moon column's glyphs
coloured by kind (each colour at least 4.5:1 against every row tint). The now row is scrolled under the frozen header once per station or point (not on a
unit or zone change; deferred while the window is minimised, in Graph view or in Summary mode). The Detailed | Summary
buttons show only in Table view and only when the payload carries a summary; the detailed table, the summary and the graphs each keep their own place in the window's one scrolling body, down and sideways (a panel shown for the first time starts at its top left; another station's summary starts at its first day, while the detailed table keeps its columns in view and goes to the now row). The forecast window's body has no top or side padding, so the frozen header and the sticky columns sit at offset 0 (a sticky table header at a negative offset let a few pixels of the scrolled rows show above it in Chromium). The compact table's borders are SEPARATE (`border-collapse: separate`, each cell drawing its top and left edge, the frozen Time column and the right-edge `col-last` cells their right edge and the last row its bottom, all in their row's style): Chromium paints a frozen cell's collapsed borders where the cell would have been, which left 1-px slivers of the scrolled rows beside the frozen Date / Time columns and under the frozen header; `--date-w` is the Date column's rendered, fractional width for the same reason. The charts: night bands at
the real times (`makeNightShade`; between first light and last light there is no band), the combined series named "Significant Wave Height", and the direction axis fixed
0-360 with compass labels N NE E SE S SW W NW N. (A sky strip with sun / moon glyphs and a "now" line were tried in 1.16.0
and removed: the owner found them clutter.) An older cached payload with none of these fields draws as before (the
hour-rule shade, no mode buttons).

## Live buoys: background refresh (plan section 36, UI asset 1.17.0)

The "Live buoys" layer merges ten agencies' station lists (NDBC, CDIP, QLD, AODN, AusWaves, Marine Institute, CEFAS,
SMHI, RWS, Copernicus). Until this change the list route asked every agency whose list had expired INSIDE the visitor's
request and answered only when all of them had: every 30 minutes one visitor waited for a refresh, after a restart every
visitor waited for every feed, and one slow agency held everyone's markers (the Marine Institute's ERDDAP once timed out
three times: 124 s). Now nothing a visitor asks for waits for an agency.

The server (`buoy_sources.py`, `app.py`):
- Each provider keeps the list in hand and refreshes it in the background (stale-while-revalidate): due at 90 % of its
  list TTL, at the retry moment after a failure (300 s, also for a first failure) or at the full TTL. A failed refresh
  keeps the last good list for up to 6 h; an empty list after a good one counts as a failure. Only a provider with no
  list at all makes a caller wait (one fetch shared by every waiting caller).
- One daemon thread, `live-scheduler`, starts with the FIRST REQUEST the process serves (Render's health check makes that
  the first seconds after a start), never at import: a gunicorn master that preloads the app forks its workers after the
  import, and a worker would inherit the locks the master's threads held but none of the threads. (Any forked child also
  resets every lock and flag of the service: `os.register_at_fork`.) Its first pass queues every agency, cheapest first
  (NDBC, CDIP, SMHI, MI-IE, CEFAS, RWS, QLD, AusWaves, AODN, CMEMS), then it loads the time-zone finder; after that it
  runs whenever an agency publishes a new list (it is woken) and at least every 30 s (2 s until every agency has answered
  once): it queues the refreshes that are due and rebuilds the merged list. Refreshes run on `LIVE_REFRESH_WORKERS`
  background threads in that order.
- `/api/buoys/live-stations` never builds and never waits: it serves the merged list the scheduler built last (the merge
  and the per-buoy time zones cost seconds of CPU; built inside requests, the requests queued behind one build held every
  server thread). While some agencies are not in that list yet (the first seconds to minutes after a start) the answer
  says so in the header `X-Live-Stations-Partial: AODN,CMEMS`, with `Cache-Control: no-store`; complete answers keep
  `public, max-age=900` and the ETag.
- `/healthz` (never cached; starts no work beyond the first request a process serves): per agency the list's version,
  size, age, next refresh, the last error and the last refresh's duration; the merged list's age, build time and whether
  it is complete and current; the scheduler's passes and errors; the refresh queue (waiting / running); the process
  memory. **Render's rules decide its shape**: an instance whose check fails for 15 s gets no traffic and after 60 s is
  RESTARTED — a running or just restarted instance too, not only a deploy (render.com/docs/health-checks). So it answers
  200 as soon as the process serves pages and the scheduler has completed a pass (partial buoy lists are a normal state:
  the page shows the list it remembers and polls). It answers 503 only: before that first pass; within
  `LIVE_WARM_DEADLINE_SEC` (10 s, never more than 14) of the start while the served list is still incomplete (a deploy
  then keeps the old instance a moment longer when the feeds answer fast); when the scheduler has not completed a pass for
  180 s (`"stalled": true`); after three failed list builds in a row (`"build_failing": true`). The last two are faults a
  restart cures, which is what Render then does. Set it as Render's Health Check Path (Deploy to Render, step 6) on the
  production AND the test service, once the deployed code has the route.
- A buoy window opened from the remembered list while that buoy's agency is still loading after a start gets
  `503 {"retry": true}` with `Retry-After: 5` at once from `/api/buoys/<id>/latest` (no request ever waits for a feed);
  the window keeps its spinner, says why, and asks again for up to two minutes; then it says the buoy is not available
  yet. The Marine Institute's per-station fallback (a buoy missing from its list) asks its server only while the list's
  last refresh succeeded, and then once with short timeouts (3 s / 8 s), never through the retrying session.
- The agencies' own log lines (each refresh's time and size, every failure) now reach the service log
  (`buoy_sources` logger).

The page (`static_ui/livelist.js`):
- Draws the list this browser saw last at once (localStorage `allshore.liveList.v1`: positions, names and what the
  detail panels need, no observations; at most 48 h old), with the note "cached list" beside **Live buoys** in the layer
  legend, then replaces it with the fresh list (unchanged markers are left alone).
- Takes the request the page started in `<head>` once; every request has a deadline of 8 s (16 and then 30 s after
  consecutive timeouts, so a steadily slow server is not starved); failures are retried after 2, 4, 8, 16, then every
  30 s, only while the tab is visible ("retrying…").
- A partial answer is drawn together with the remembered markers of the agencies still loading ("loading more…" once
  something is on the map), is never stored, and is asked for again after 3, 3, 5, 5, 10, 10, 15 s, … (40 times), then
  once a minute for two hours. A complete answer is stored.
- "loading…" and "cached list" appear only when the list has not landed within 300 ms, so a normal load never makes the
  legend grow a line and shrink back; the note is a visual hint (`aria-hidden`), the checkbox keeps the name
  "Live buoys".
- A `performance` mark `allshore:live-first-markers` (detail: cached / partial) records when the first markers were drawn.

Environment variables (all optional):

| Variable | Default | Meaning |
|---|---|---|
| `LIVE_BACKGROUND` | `1` | `0` turns the background service off: the route then asks every expired agency inline, as before (the tests and `start.sh`'s import check use this). |
| `LIVE_REFRESH_WORKERS` | `3` | Agencies refreshed at the same time (1-10). |
| `LIVE_TICK_SEC` | `30` | Seconds between the scheduler's passes once every agency has answered (1-600; a bad value falls back to 30). |
| `LIVE_WARM_DEADLINE_SEC` | `10` | `/healthz` waits at most this long after a start for the full list (clamped to 0-14: Render cuts traffic after 15 s of failed checks). |
| `LIVE_STATIONS_EDGE_TTL` | `0` | Seconds Cloudflare may keep a complete list (`CDN-Cache-Control: max-age`); capped at 300; `0` = never (`no-store`). Partial answers are never stored. (Before section 36 the value `1` switched on a lifetime computed from the lists' ages; it now means one second.) |
| `LIVE_BREAK_PROVIDERS` | empty | Diagnosis only (test site): a comma list of agencies (e.g. `AODN,CMEMS`) whose list fetch fails on purpose, to watch the kept lists, the retries and `/healthz`. Never set it on production. |

Gunicorn: the service runs one scheduler per worker process; the site runs one worker (with threads), so each agency is
asked once per refresh. More workers would each run their own scheduler (correct, but more feed traffic).
