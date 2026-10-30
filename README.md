# NOAA GFS Wave Table View Web App

This repository contains a Flask web application that fetches the latest NOAA GFS wave `.bull` file for any available buoy station, parses the data, and displays it in the same format as the **Table View** worksheet from an Excel file provided by the user. The output includes a metadata row, two header rows (parameter names and units), a blank row, and the data rows.

## Features

- **Buoy Dropdown**: The home page includes a dropdown menu preloaded with the NOAA GFS stations available in the `.bull` directory (see "The forecast-station list" below). You can select any buoy station to view the latest model data.
- **Automatic Run Detection**: The application automatically determines the most recent model run (18z, 12z, 06z, or 00z) by probing the NOAA directory structure for the selected date and hour. If the latest run isn't available, it falls back to earlier runs.
- **Excel-Style Table**: The data is formatted to mimic the two-level header structure found in the provided Excel **Table View** sheet, including a metadata row for cycle information and units row for each parameter. A blank separator row is also included for clarity.
- **Forecast points**: Map tools -> **Forecast point** gives the same table and graphs as a buoy station for any
  point on the open sea, titled with its coordinates, and keeps it under **My points** in the visitor's browser (see
  "Forecast points" below).
- **Tide stations**: NOAA's tide-prediction stations appear on the map from zoom 9 (sine-wave markers); a click opens
  a tide window: a day strip for 30 days (a column per day, today open, a click opens or closes any day) with the
  predicted curve, the highs and lows, the nights, the sun and moon, the current time and, where the station has a
  gauge, the observed water level of today so far (see "Tide stations" below).
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
- `gfs_bulletins.py` + `gfs_bull_index.json` — the GFS station bulletins from NOAA's run archive on AWS (see "GFS
  bulletins and winds: where they come from"); `tools/build_bull_index.py` builds and checks the index.
- `point_forecast.py` — reads the forecast-point product for one ocean point: the water / land test, the model cell,
  the rows and the swell rank (see "Forecast points: the reader").
- `tools/model_frames/points.py` and `pointfmt.py` — the GitHub Actions job that publishes the forecast-point product,
  and its file format (shared with the reader).
- `buoy_sources.py` — the live-buoy providers (one per agency) and their background refresh; `static_ui/livelist.js` —
  the page's live-buoy list loader (see "Live buoys: background refresh").
- `tide_sources.py` — NOAA CO-OPS tide predictions per station (the curves, the cache); `tide_stations.json` — the
  station snapshot (`tools/tides/fetch_stations.py`); `static_ui/tides.js` — the tide window's chart (see "Tide stations").
- `README.md` — This file. Provides setup instructions and describes the features of the project.

## Contributing

Contributions are welcome! If you want to add new features, improve the parsing logic, or update the UI, please submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## Forecast window client (static_ui)

`static_ui/forecast.js` is the page's own client module for the forecast window (plan section 25). It is served at
`/ui/<name>?v=UI_ASSET_VERSION` (app.py), always (not behind the overlay flag, never under `/overlay/`), immutable for a year
like the overlay assets: any other `?v` is a 404 with `no-store`. Bump `UI_ASSET_VERSION` on every change to `static_ui/*`
and update `tests/fixtures/ui_assets.json` (version + sha256); `tests/test_ui_module.py` checks the route, the pin, and runs
the Node tests in `tests/ui/`. `/api/forecast` also reports `model` (the model actually used: SWAN falls back to GFS off the
17 SWAN stations; a Great Lakes station is always `GLWU`) and `swan_available`; `?compact=1` asks for the window's table.

The top-right controls (plan section 54, UI 1.26.0) are one row of three buttons: Legend (a key), Tools, Settings. The
layer legend (Leaflet's layer control: the layers and their notes) lives in the key's panel, closed on every load and
never remembered; the panel opens and closes like the gear's (an outside press, another button or Escape, which puts
the focus back on the key). While the legend is closed, a dot on the key says a legend note exists (the live list
loading, a tide or wind list unavailable, the NWS feed stalled) and the key's label reads "Legend · <note>". Each button
names itself at once on mouse hover and on keyboard focus (never on touch, never while its own panel is open). The
row sits 26 px down, below the longitude labels along the map's top edge (owner: a label is never pushed down around
the buttons), whether the gridlines are on or off. The corner's measured height and width (`--map-topright-h`, `--map-topright-w`) count only the corner's own controls (the
row, and the map tools' bar while shown), so the legend inside its panel never moves a window.

The page (2026-09-26, plan sections 25 and 26): the map is the page — the brand sits on it top-left above the
model-overlay selector, the settings gear (Time Zone, Units, lat/long gridlines; with the model overlays also "Pressure
isobars (wind)" and a "Particles" slider, plan section 46: see below) top-right, the
credits behind an (i) at the bottom-left (the Home view button is gone: plan section 46); the forecast window's heading is the station picker with favourites
(expanded and minimised); the live-buoy panel is a second floating window (`#liveBuoyPanel`, `createLiveWindow` in the
module: dragged, resized, minimised to a chip at the bottom-right or a bar above the forecast bar on phones, closed
from any mode; `sessionStorage 'allshore.liveWin.v1'`); the tide-station window is a third one of the same kind
(`#tideWin`, `createTideWindow`; `sessionStorage 'allshore.tideWin.v1'`; its chip stacks above the live-buoy chip, its
phone bar above the live bar; both station windows can be open at once, the one touched last on top). The forecast is a floating window (`#forecastWin`) that starts minimised in
a new tab (a chip at the bottom-left on desktops, a bar along the bottom edge in phone mode = a viewport 500 px or
less wide OR tall; expanded it is a window on desktops and full-screen in phone mode; on desktops 1280 px and wider
it opens between the top-left controls (the logo and the overlay panel, measured: `--map-topleft-r`) and the
top-right row of buttons (`--map-topright-w`; the map tools' bar counts while shown), from 96 px down to 16 px above the
bottom edge, its bottom raised over parked chips; narrower desktops keep the bottom-right box; plan section 46), holds the View (Table / Graph) and Model (GFS / SWAN, only on SWAN stations) controls
in its toolbar, and is dragged by its header, resized from any edge or corner (the bottom-right grip also takes the arrow keys), maximised below the map's top-right controls and minimised;
Escape closes the tide window first, then the live-buoy panel, then minimises the window. Every change (a marker or favourites pick, model, view,
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

## Where the site opens (plan section 46, UI asset 1.23.0)

A fresh tab opens at the visitor's own area: the head asks the geo Worker (`tools/geo`, `GET https://geo.allshoresurf.com/`,
Cloudflare's `request.cf` position for the IP address: the ISP's city; nothing is stored anywhere) and the station list, picks
the station nearest that position (haversine, so the date line is no seam) and starts its forecast request (a visitor on Oahu,
inside a box around the island, 21.2-21.76 N and 158.32-157.6 W with no other island in it, gets Waimea Bay 51201, the
forecast most Oahu surfers want, though another station may be nearer: owner, 2026-10-18; `PREFER` in the head); the map opens
centred on the visitor at zoom 6, zoomed out until that station is in view (15 % padding; never below the map's minimum
zoom), and the forecast window loads the station. The Worker's answer and the list are raced against a 2-s cap; while it runs
the map pane is hidden (no Hawaii tiles paint; the navy background shows under the controls) and a visitor who moves the map
or picks a station meanwhile keeps what they did. Every failure (no Worker, a slow or refused answer, a VPN exit on land far
from the sea) lands on today's behaviour: Hawaii and 51201. Precedence: a station in the address (`?station=`: every reload,
since the address is written after the first load, and every link) > the viewer's default station (the shaka in the
favourites picker, localStorage `allshore.defaultStation.v1`: its position at zoom 6) > the IP's nearest station > 51201.
The default and the favorites (owner, 2026-10-18/19): the top of the forecast window and every saved row of the dropdown
(Favorites and My points) carry the same toggles, hollow and grey when off: the shaka ("Default: site opens here"; the
owner's own drawing, skin toned when on; one default at a time, listed first at the next opening) and the star
("Favorite", gold when on; for a point: kept in My points); each row also has a ✕ ("Remove") that clears both. The two
are independent: a default need not be a favorite (it stays listed). A row stays in its place until the list closes,
also once both are off, so a slip is one click back. On phones a row's buttons sit on a line of their own under the
name, so a point's coordinates are never cut, and the window's own pair moves from the name's box to the open window's
toolbar (owner, fix round 5): the bar and the full-screen header show the name whole; one state for both pairs. A point's
pair waits for its forecast on the page (a linked point still loading cannot become the default); a write the browser
refuses (storage switched off) is said and changes nothing; a point let go with either star comes back with its name;
for the map: the tab's own view (sessionStorage) > the default station's position > the IP position > Hawaii. The head and
the module ask for one and the same query (tests/ui/early.test.js); the module's first load waits for the head's answer
(`window.__early.station`) and a load the viewer causes meanwhile wins. Env `GEO_BASE` (e.g. `https://geo.allshoresurf.com`)
turns it on; unset, the page opens as before. The Home view button is gone (its saved view is no longer read).
G32 fix round (UI asset 1.23.1): while the race runs nothing is kept as the tab's view (a reload then asks again); a
pointer, wheel, key or touch on a control over the map (the overlay panel, the gear, the legend, the overlay's sheet) is not
"moving the map", and moving the map shows it at once; the default station's id follows one rule everywhere (trimmed, 1-32
letters, digits, `_` or `-`: a NOAA id or a point's `pt_` id); a default the station list no longer has (and not a point) is
forgotten once the list is in, and the window says so once (a `pt_` default is judged only once forecast.js has run: the
list often lands first, and a default point was erased that way until 2026-10-18); the chip names the race's station as soon as the race answers.
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

The browser talks to the bucket directly; the Flask worker only serves five immutable assets
(`/overlay/overlay.js?v=...`, `/overlay/overlay.css?v=...` and the speed selector's `snail.png`, `fish.png`, `shark.png`,
all at the same `?v=`). Bump `OVERLAY_ASSET_VERSION` in `app.py` on every change under `static_overlay/` (any other `?v`
is a 404 that is never cached); `tests/fixtures/overlay_assets.json` pins the version to a hash of all five files (the
PNGs hashed raw; `.gitattributes` marks them binary).

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
manifest). `pres` (plan section 46) is the GFS sea-level pressure (PRMSL, the pressure reduced to mean sea level; not
MSLET) in hPa, 920-1060 hPa (0.55 hPa a code; owner 2026-10-17: a wider range moved the 2-hPa isobars noticeably in
flat highs), half resolution only and never filled: the client draws isobars from it over the wind overlay. About 57 KB
a step (+12 MB a GFS run, +4.9 MB an ECMWF run); the run summary prints the run's lowest and highest pressure and how
many cells clamped (a typhoon eye deeper than 920 hPa loses its innermost rings; the reduction over the Antarctic plateau
or a winter Siberian high above 1060 shows no lines inside the patch). A run published before `pres` existed may be
rebuilt with `force`: the rebuild guard lets an old manifest lack only the fields in `run.ADDED_FIELDS` (`pres`), so the
rebuild adds `half/pres/*` and rewrites the other fields byte for byte; a changed range, a missing, extra or renamed
field, or an empty field block is still refused. The job records per step how many cells have at least 0.1 m of waves but no direction, or a direction
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
list (the GFS frames, the ECMWF frames and the forecast-point product below); GitHub's own schedule stays on as a
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
sessionStorage (`allshore.overlay.v1`, with the playback speed: 1x / 2x / 4x, shown as a snail / fish / shark; a 0.5x
from an older save plays at 1x), so picking another forecast
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
Lake outlines (asset 2.22.0, plan section 51): under the wind overlay the shores of every lake of 100 km² or more
(and of the islands of 100 km² or more in them) are drawn as the same thin line as the coastline, from tile zoom 4. They
come from `static/coast/lakeoutlines-v1/` (built by `tools/coast/build_coast.py --set lakeoutlines`, see tools/coast/README.md;
overview file `lakeoutlines-i.bin`, full-resolution 5° cells from tile zoom 7), loaded the first time wind is shown at zoom 4
or more and never holding a draw. The gear row "Lake outlines (wind)" turns them off (on by default; localStorage
`allshore.lakeOutlines.v1`, "0" = off). Wind is never clipped, so the readout is unchanged; wave height, period and SST are
untouched. River-lakes (wide river reaches GSHHG marks as lakes) are not outlined.
The isobars' look (asset 2.19.3, the G32 fix round; the owner asked for Windy's look): the pressure field under the view is
averaged into blocks at least 7 px apart on screen and smoothed twice by a [1, 2, 1] kernel, so the lines are smooth and the
small rings of mountain pressure reductions and 8-bit terraces go (on a real GFS frame the large systems' lines stay within
55 km of the true isobars for 95 % of their points and within 110 km for 98.6 %; deep lows read a little shallower in the
lines, the readout's "· 1004 hPa" stays the exact value at the cursor). Spacing: every 8 hPa while the whole world is in
view (any screen width) or to zoom 2.9, every 4 hPa to zoom 5.9, every 2 hPa beyond. Each isobar has at most one small
pill (9-px text), rotated along the line, from zoom 4 and never while the whole world is in view; highs and lows are marked
H and L with their pressure, found on the frame's own values (not the smoothed lines) and chosen by PROMINENCE (asset
2.20.0, the G32 re-check; owner's calls 2026-10-18): a node that nothing within ~110 px on screen undercuts (L) or overtops
(H), whose patch of equal values is marked once at its middle (also when the patch runs out of the view at zoom 8-9: asset
2.20.1; no mark for a patch that runs down to a lower value, holds
more than 4,096 nodes or spans half the world), and whose basin must rise (L) / fall (H) at least 1.1 hPa (two codes)
before a stronger centre is reached (the merge tree of the frame's codes over the view and a margin, on the lines' node
spacing at wide views, coarsened so that a window holds at most 16,000 blocks; the strongest centre of a region always
counts); one mark per ~110 px among the marks in view, the most prominent first (a twin beyond the view's edge hides
nothing). No mark on high ground: `terrain_half.png` (NOAA GFS's model terrain on the half grid, the highest 0.25-degree
node of each half node's footprint, built once by `tools/model_frames/make_terrain_mask.py`, 20 m per code) at or above
400 m at the patch's first node or its middle — the sea-level pressure both models reduce from the surface leaves a fixed
bump on every mountain node (an "H" on
the Big Island at every step before; on 22 live frames 400 m leaves no persistent mountain mark on either model, 500 m
left Guadalcanal, Socotra and New Britain). One exception (asset 2.20.2, the owner's call): a LOW at least 15 hPa prominent
over ground below 2,000 m at both nodes keeps its L, so a typhoon crossing Taiwan or Luzon stays marked; the mountain bumps
were at most 11-14 hPa, all highs. Its strength is judged over the WHOLE grid (asset 2.20.3, `strongLowSearch`: a minimax
search from the low's patch up to 15 hPa, cached per frame and patch; more than 20,000 nodes counts weak), never by the
view: before, a weak low alone in a zoomed-in window counted as strong (about 1,100 weak marks at zooms 5-10 over 294
live frames, none now). On the GFS 2026101900 and ECMWF 2026101812 runs (294 frames) the exception brings back 52 GFS and
26 ECMWF lows (77 on the GFS 18Z run before it), none persistent, no high. A high over high ground (a Mongolian winter
high) and a weaker low there have no mark, at any zoom. A typhoon below the stored range (920 hPa) marks "L ≤920" when its clamped patch is
compact and within 70° of the equator (a high above 1,060 "H ≥1060"; the polar plateaus never), and the hover reads
"≤ 920 hPa" there. The lines are cleared under each mark's box, so an L stays legible inside packed rings. So a compact
typhoon keeps its real centre and central pressure (asset 2.19.4: on the smoothed field a 956-hPa typhoon read "L 1000" at
the world view), a second low along a trough keeps its L (2.19.5), broad highs keep their H at every zoom and one-code
dimples mark nothing (2.20.0; before, a depth against a ring 110 px out lost the Tasman high's H from zoom 6 and marked
dimples 0.55 hPa deep). Lines are thin and faint (a 1.2-px white line at 72 %). A step waits at most 1 s for its pressure
frame (owner, G32), counted per step (a goto of the same step issued again, as a zoom across the half / full threshold
does, waits only the rest): a slower one lands without lines, which appear when the frame arrives. One frame (at most
~16 ms) of the previous step's lines can show under the new picture when the browser paints between a landing and the
next animation frame (G32 A-P3-5, accepted).
Late frames (asset 2.14.12): the 5-s watchdog (or 4 playback intervals) times a frame's download only; a download that
stalls is asked again at once, three attempts in all, and only then is the frame left out for a minute and listed as
"Unavailable frames are skipped". A frame that is merely slow (a busy page at fast speeds) is waited for, never listed.
Since asset 2.22.4 the watchdog counts only the time with no byte arriving (every chunk re-arms it), for every frame: on a
slow link (30-45 KB/s) a direction or full-resolution frame that is still arriving is no longer thrown away after 5 s in all.
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
Animation (asset 2.9.5; always on since asset 2.12.0, no checkbox): the overlay draws
particles with fading trails that flow along the dominant swell direction under wave height and peak period, and along
the wind under wind speed, on a canvas of their own under the forecast points (no pointer events; markers keep their
taps). The directions come from the `pdir` / `wdir` frames of the same step, loaded through the frame scheduler
beside the field frame (the target's field frame first, then its direction, then the ring of frames around it, never more
downloads at once than the speed's budget, two when paused or at 1x; `pdir` at the 0.5 degree frames at the default zoom 6 and wider and the 0.25 degree ones as
soon as the map is zoomed in, from 6.5 (back below 6.25), so near coasts the direction comes from the finer grid; `wdir`
at 0.5 degrees only). Data per full 209-frame loop with Animation on (scaled from run 2026092518): wave height or period
+ direction about 53 MB on desktops at the default zoom, 85 MB zoomed in; 30 MB on phones at the default zoom, 62 MB zoomed
in to 7.5 and 85 MB beyond; wind + wind direction 59 MB (119 MB past zoom 7.5). A 1x loop takes about 105 s (the hourly
first five days at 2 h/s, then 6 h/s). The timeline (asset 2.14.0) is a compass ribbon: the run's dates and hours, laid out
by the frames' own times in the forecast's zone (3-hourly frames three times as far apart; day labels at the local midnights;
hour ticks at 00/06/12/18, thinned when the scale is small), scroll under a fixed coral pointer, and the selected time sits
above it with its weekday ("Sun, Oct 11 · 7 AM HST"; while a frame loads it names the REQUESTED time with "loading…", or "unavailable" when that
frame is missing, the picture staying on the frame drawn). Drag or swipe the ribbon (it follows the finger one to one and snaps
to the nearest real frame; a tap picks the frame under the finger), scroll it sideways with the wheel, or use the keys on it
(arrows one available frame, Home / End the first / last available frame, PageUp / PageDown a day; manual navigation stops
at the ends, only playback loops). Every scrub pauses playback and goes through the ordinary seek, so
obsolete fetches are cancelled and a late answer never replaces a newer choice. A thin overview slider beneath it spans the
whole run (it snaps to a frame in the direction of travel); the run's length is not shown (owner). The panel's
metadata lines give the run ("Run: 2026-10-07 06Z (UTC)", with the forecast
table's run when it differs), when it went live ("Updated: 1:33 AM HST") and when the next is expected ("Next Update: about
7:35 AM HST": one cycle after this run's own publish time; "expected shortly" once due; "a newer run is available" with the
Update banner), in the computer's own time zone (asset 2.14.0; was one "live since … · next update about …" line). The head row
(desktop panel and phone sheet alike) names the model while the panel is open and, folded, shows one line "Thu, Oct. 8, 06:00
PM HST" (weekday, date with a period after an abbreviated month — never "May", never another locale's shape —, the table's
clock, zone; a pending seek adds "· loading…" or "· unavailable"; assets 2.14.3-2.14.4, owner 2026-10-07: no field name, no
hour count; before: "+213 h → +9 h loading…" on phones). A direction is shown only for the step on the map: a step change clears the animation until that step's
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
zoom; the Home view button that stayed then was removed in plan section 46); with the overlay flag unset the page is unchanged.
Panel redesign (asset 2.14.0, owner 2026-10-07, plan section 37): the control is ivory with navy text and teal controls in the
logo's palette (14 px corners, up to 340 px wide on desktops); the "Model overlay" heading is gone (the select keeps it as its
accessible name) and the head row shows the model's name from the manifest without its agency prefix ("GFS-Wave (WAVEWATCH
III)"; asset 2.14.3, owner; NOAA stays in the map's attribution). One
play / pause button sits beside the timeline (no first / previous / next / last buttons). The playback speed is an
illustrated selector: a button with the chosen animal (the owner's art: snail 1x, fish 2x, shark 4x; 0.5x dropped) opening a
listbox of the three, keyboard-operable (arrows, Home / End, Enter / Space, Escape, Tab), closed by a press outside; the
speeds appear only in tooltips and accessible names. While playing, the chosen animal bobs (snail) or swings its tail (fish,
shark); not under reduced motion. The legend strip is taller with clearer labels (the colours and tick positions are
unchanged). On phones the sheet opens with its details when 40 % of the map is at least 110 px (else the one-line summary,
as before). Asset 2.14.1 (test-site fixes): the keys stop at the first and last
available frames; a field switch while a scrubbed frame is still loading keeps the requested time (Retry still goes back to
the frame on the map); a pinned first-day label is dropped unless the next date is at least 60 px away; and the panel follows
window resizes and rotations by watching the window and the map container (the page has Leaflet re-read the map size before
`invalidateSize()`, so the map's own `resize` event does not fire there; before this the sheet stayed a sheet on a window
grown to desktop size, and the attribution width and frame resolution did not follow either). Asset 2.14.2: with no room
for the details (a phone turned sideways, a short window) the details go and the transport row (play, ribbon, speed) stays;
with no room for that either, or when the viewer folds the panel, the one play button sits in the head row instead (as the
old panel's head button did); a fold for lack of room is never saved, and until the viewer uses the toggle the size decides
at every render (a phone turned upright opens up again); a new sheet takes its place beside the zoom column before its
ribbon is measured (else the pointer and the label could disagree by hours after a resize). Asset 2.14.5 (the G26 fixes,
record `docs/reviews/overlays-G26-adversarial.md`): a day label wherever the local date changes, so a clock change AT
midnight (the Azores, Havana, Santiago, Beirut, Cairo) neither drops a day nor labels one twice; a tap whose finger rolls a
pixel before lifting is no longer undone by the scrub queued for the next frame; a gesture the browser cancels is never a
tap, and the ribbon takes no browser panning (`touch-action: none`); on desktops the room for the details is measured from
where the control really starts (below the brand), and a height change re-renders the panel; the toggle keeps the
keyboard focus through a fold; only the loading / error states and the warning lines are live regions; the run line
refreshes once a minute; the speed button names its listbox; an empty or unknown zone reads as the computer's own (as the
page's clock); the coral pointer has a solid navy edge (3:1 on the cream ribbon); 24 px+ toggle and slider on touch. Asset 2.14.6
(seen on the test site with real touch input): a swipe up or down the ribbon (10 px or more) is never a pick. Asset 2.14.7: such
a release (and a cancelled gesture) with under 4 px of sideways drift goes back exactly to its own frame. Asset 2.14.8 (the
G26 re-check): a gesture on the ribbon is one pointer, the one that pressed, and its direction is decided once it has
travelled 10 px, by the larger of the two: sideways is a scrub (playback pauses then and frames are asked for from then
on), up or down is a swipe that is never a pick (the ribbon goes back, nothing is loaded, playback carries on); before
that the ribbon follows the finger but loads nothing, and a release is a tap (the frame under the finger where it touched
down) or a short drag; a second finger ends the gesture with no pick, back to the frame it started on. The panel opens as
far as the room allows (measured, no fixed height) and offers the toggle only when opening shows more than its one-line
head; keyboard focus stays on the same control through every re-render; the loading, error and unavailable-frame
messages are read out once through one hidden `role=status` node (nothing in the panel itself is a live region).
Asset 2.14.9 (the short fresh check of 2.14.8): a tap reads the touch-down point against the ribbon as it was at the press
(also at the run's ends, where the ribbon cannot follow a roll); the point where a press is released decides its direction
like a move; a press during the ribbon's step glide starts from where the ribbon is seen; keyboard focus survives the
loading and error panels (Retry, Update) and the details scroll box keeps it; the status node goes in empty and its first
text follows a moment later; a touch whose release never arrived no longer blocks the next one.
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

### ECMWF frames (plan section 43, the job)

A second model for the overlays: `tools/model_frames/ecmwf.py` (GitHub Actions `model-ecmwf.yml`, its own concurrency
group, cron every 10 minutes and dispatched by the trigger Worker with the others; re-enabled by the keepalive) renders
ECMWF's free real-time open data (IFS, 0.25 degree, CC BY 4.0) through the SAME pipeline as the GFS job: `run.main` with
another `Source` (discovery, one step's records, their decode, steps, manifest model block, bucket prefix). Encoding,
encode ranges, legends, coastal fill (the same `fill_allow.png`: the grids are identical), guards, failure records and
retention are shared, so the browser draws both models with the same code.

- **Data**: the AWS Open Data mirror `ecmwf-forecasts` (anonymous listing + ranged GETs; data.ecmwf.int/forecasts serves
  the same paths and is the second host for the reads). Per step the `wave` stream's `swh` (-> `hs`), `pp1d` (-> `tp`,
  the PEAK period 1/fp, comparable with GFS-Wave's PERPW) and `mwd` (-> `pdir`: the MEAN wave direction, degrees true,
  FROM; the free data has no peak direction and no swell partitions), and the `oper` stream's `10u`/`10v` (-> `wind`,
  `wdir`) and `msl` (-> `pres`, Pa -> hPa; plan section 46). The `.index` (JSON lines) gives each record's exact byte span; every record's identity (param, surface level
  0, step, cycle) and grid are checked after decoding (CCSDS packing: eccodes 2.48 in the pinned environment). A step's
  records are read at once (`READ_WORKERS`: the mirror is in Frankfurt; a run builds in ~2 min on GitHub's runner).
  The mirror gets two quick tries per read for an outage-type error before data.ecmwf.int; after such a failure it is
  skipped for 10 minutes (`AWS_COOLDOWN_S`). Run discovery lists the mirror only. THROTTLING is normal on both hosts
  (the mirror's "503 Slow Down", data.ecmwf.int's 429; worst right after a cycle is released) and never counted as an
  outage. A throttled MIRROR read goes to data.ecmwf.int at once and the mirror rests 5 minutes
  (`AWS_THROTTLE_COOLDOWN_S`); on data.ecmwf.int (two reads at a time) a throttled read is retried after 5, 10, 20, 30,
  45, 60 s (or the server's Retry-After, at most 60 s). A read or a listing still throttled after that ends the tick
  quietly (exit 3, `notready/` count, no failure record, no email); the next tick tries again. A build still fetching 45
  minutes after the job started (`BUILD_BUDGET_S`) also ends the tick quietly, before the job's 60-minute limit would
  stop it. A real build failure backs off 1 h, up to 6 times (GFS: 3 h, 3 times). The first live 00Z build
  (2026-10-15 07:36 UTC) failed on 429s before these rules and went live 3 h late; the 12Z build that day waited out
  the throttle ladder on the mirror (~3 min a step) and was stopped at 60 minutes.
- **Calm water and sea ice** (G30a F1): ecWAM keeps the sea-ice pack as wave points with a near-zero height and a
  meaningless "peak period" (up to its 29-s ceiling), where GFS-Wave leaves ice empty. The job leaves `hs`, `tp` and
  `pdir` empty wherever `swh` is below 0.1 m (`ecmwf.mask_calm`; the GFS job's calm line for directions): ~80k cells a
  step, 99 % poleward of 55 degrees, where GFS's frame is empty on 89 % of them; the waves ecWAM carries into the marginal
  ice zone stay (GFS draws most of the under-ice cells with 0.5 m or more). Wind is never masked. Two limits, both like GFS's:
  waves of 0.1-0.4 m that ecWAM keeps under the pack can still carry a long period (f072 of run 2026101412: ~900 such
  cells at 22 s or more, GFS 79), and the coastal fill (one cell from GSHHG land) can paint emptied coastal pack ice
  from those neighbours (~3-4k cells a step poleward of 60 degrees).
- **Runs**: the 00Z and 12Z cycles only (owner, 2026-10-14), each to +360 h: every 3 h to +144 h, every 6 h to +360 h
  (85 frames; the manifest's `frame_schedule` `[[0, 144, 3], [150, 360, 6]]`). ECMWF publishes every step of a cycle at
  once about 7 h 34 min after it (00Z at ~07:34 UTC, 12Z at ~19:34 UTC); a run counts as complete only when every
  `.grib2` and `.index` of both streams is listed.
- **Grid**: ECMWF's columns start at 180 E (= -180 E), the site's column order, so there is no roll (central Oahu's
  land cell sits at the GFS frames' row 274, column 88). ECMWF has said its open data moves to 0.1 degree; a 0.1-degree
  message is regridded to the site grid (bilinear over present neighbours, nearest for `mwd`) so the frames and the
  client never change (`decode.to_site_grid_ecmwf`). Finer frames would be a client redesign of their own. The object
  paths carry the resolution (`ifs/0p25/`; ECMWF used a new segment per resolution before), so a switch needs the path
  changed in `ecmwf_fetch._prefix` as well. Until then the job finds no NEW run. Once the live ECMWF run was published
  30 h ago or more (`stale_fail_h`; runs land every 12 h), whether the job then finds no complete run or only the live
  one again, the run FAILS (exit 1, the workflow fails, GitHub mails) with a summary WARNING, at most once every 6 h
  (`STALE_ALARM_EVERY_H`, record `ecmwf/0p25/v1/stale-alarm.json`); between alarms it exits as usual (G30a F2, R3).
  An unreadable pointer while no run is found is a failure too.
- **Bucket**: `ecmwf/0p25/v1/` with the GFS layout (`<RUN>/{half/}<field>/fNNN.png`, manifests, stats, `latest.json`,
  `failed/`, `notready/`); the job never reads or writes the GFS prefix and prunes only its own runs (4 kept, ~125 MB
  each). The manifest's `model` block carries ECMWF's notice block (G30a F4): "(c) <year> European Centre for
  Medium-Range Weather Forecasts (ECMWF). Source: www.ecmwf.int. Licence: CC BY 4.0 (link)", what was modified
  (quantised, calm water and sea ice left empty, coast-filled, rendered), ECMWF's liability disclaimer and that ECMWF
  does not endorse the product. Stop it by disabling `model-ecmwf.yml` (and removing it from the keepalive
  and the Worker's `GH_WORKFLOW`); the client shows ECMWF only where `MODEL_OVERLAY_MODELS` names it.
- **Fixtures**: `tests/capture_ecmwf_fixtures.py` (network + eccodes) writes `tests/fixtures/ecmwf/` (the real index
  files and each record's metadata and a few cell values, incl. three sea-ice cells) for `tests/model_frames/test_ecmwf.py`.
  Review record: `docs/reviews/overlays-G30a-adversarial.md`.

### Great Lakes frames (plan section 50, the job)

The GFS-Wave frames have no Great Lakes data. `tools/model_frames/glwu.py` (GitHub Actions `model-glwu.yml`, its own
concurrency group, cron every 10 minutes and dispatched by the trigger Worker with the others; re-enabled by the
keepalive) renders NOAA's Great Lakes Wave Unstructured run (GLWU: WAVEWATCH III on an unstructured mesh, written on the
NDFD 2.5-km Lambert grid) through the same pipeline (`run.main` with a third `Source`). The browser draws it over the
lakes under the GFS model selection (plan section 50's client steps).

- **Data**: NCEP's operational long run on NOMADS (no AWS mirror exists): `glwu.YYYYMMDD/glwu.grlc_2p5km.tCCz.grib2`,
  cycles 01/07/13/19Z, hourly to +149 h (150 frames; `frame_schedule` `[[0, 149, 1]]`), posted ~26 minutes after the
  cycle hour. A run counts as complete when its `.idx` lists every needed record of every step. Each step's records
  WIND, WDIR, UGRD, VGRD, HTSGW, PERPW, DIRPW are adjacent: ONE ranged read a step (~300 KB). Fields: `hs` = HTSGW,
  `tp` = PERPW (1/fp, the GFS-Wave encoding: a true peak period), `pdir` = DIRPW (peak direction, FROM). The waves are
  driven by the NWS forecasters' NDFD winds, not GFS: on the lakes they can disagree with the GFS wind animation.
- **NOMADS politeness**: every request goes through one pacer (one at a time, 0.6 s apart: under NOMADS' ~120 a minute);
  429/503 are retried after 5-60 s (or Retry-After, at most 60 s). GLWU has no second host, so throttling, an outage
  (timeouts, 5xx) and a file still being written (a short read, HTTP 416) all end the tick quietly: exit 3, a
  `notready/` count, no failure record and no mail (G33a P2-2). A long outage is reported by the stale alarm: a live run
  18 h old or more (three missed cycles) fails the workflow once every 6 h (`stale-alarm.json`). A build still running
  after 20 minutes (`BUILD_BUDGET_S`) ends the tick quietly too. A real failure backs off 30 minutes, up to 6 times. A
  whole run is ~150 GETs + 72 bulletins, ~4 minutes on this machine.
- **Grid**: every message's identity (shortName, surface level 1, step, cycle) and grid (Lambert 581x361, LoV 265, one
  standard parallel 25 N, 2539.703 m, scanning mode 64, sphere R 6371229 m) are checked. `decode.lambert_colrow` is the
  spherical Lambert projection; each build checks it against eccodes' own coordinates (within 1e-4 of a cell; measured
  3e-12). Each node of the browser grid (`decode.GLWU_GRID`: 0.02 deg, 831x401, 49.2..41.2 N, 92.4..75.8 W,
  centre-registered, not periodic) takes its NEAREST GLWU cell (no averaging: every value is the model's). Land is the
  GRIB's missing value (44,863 water cells). A water cell without a period (WAVEWATCH III leaves the period undefined
  on ice-covered and inactive points) is left empty in all three fields. Calm water (`hs` below 0.1 m, `glwu.CALM_M`)
  keeps its height but gets no period or direction (owner 2026-10-21, G33a P2-1: GLWU writes a period over flat-calm
  water, often half the lakes on a quiet day, and those cells carried the map's longest periods; particles never draw
  there anyway). A nearest-water fill of 2 nodes (`glwu.FILL_CELLS`, ~3-4 km) carries the colour to
  the lake shores, where the browser clips it to GSHHG's lake polygons; the period's and the direction's fill enters
  only cells without a height (fill version 2, plan section 50 step 6b: the first fill had copied a neighbour's period
  into calm cells). Same encoding and encode ranges as the global frames (one resolution, "full"): ~63 KB a step,
  ~10 MB a run.
- **Forecast tables**: after the frames, the job reads each station's bulletin (`bulls.tCCz/glwu.<id>.bull`, the
  GFS-Wave "day & hour" layout; 72 stations in `glwu_fetch.STATIONS`: the 45xxx buoys, C-MAN / NOS sites and named
  beaches; the rip-current points left out), checks it (station, position within 0.02 deg, cycle, 150 rows) and stores
  it as `glwu/v1/<RUN>/bull/<id>.bull`. A bulletin that is missing, bad or unreachable is left out (once NOMADS
  throttles, the rest are not asked for) and the site's table then uses the newest kept run that has it; bulletins never
  hold the frames back (G33a P2-3): more than 7 left out is a summary WARNING. A partial (`--steps`) build stores no
  bulletins or winds (immutable keys). The NDFD wind at each
  station (its own GLWU cell, else the nearest water cell within 3) goes to `glwu/v1/<RUN>/winds.json` (m/s and degrees
  FROM per step; 45195 on Lake Champlain lies outside the grid: no wind). Both are stored before the manifest (keys
  `bulletins`, `winds`), so the pointer never names a run without them. The site reads them from R2, never NOMADS.
  Conventions: the frames' `pdir` and `winds.json` are FROM; the stored bulletins keep the WW3 bulletin convention
  (direction TO), which the site's bulletin parser turns into FROM (+180), as for GFS-Wave.
- **Bucket**: `glwu/v1/` (`<RUN>/<field>/fNNN.png`, `bull/`, `winds.json`, manifests, stats, `latest.json`, `failed/`,
  `notready/`); 4 runs kept. The other prefixes are never touched. The rebuild guard compares this source's fields and
  fill. Stop it by disabling `model-glwu.yml` (and removing it from the keepalive and the Worker's `GH_WORKFLOW`).
- **The lake shores**: `tools/coast/build_coast.py --lakes` builds `static/coast/lakes-v1/` (coast-v1 format, the same
  tiers; `"kind": "lakes"` in its index): GSHHG's three Great Lakes polygons (level 2, >= 10,000 km2 in the lake box:
  Superior + Michigan + Huron + St Clair, Erie, Ontario) counter-clockwise and the 139 islands they contain (level 3)
  CLOCKWISE, so under the browser's nonzero fill an island cancels its lake. 9.6 KB overview file + 7 cells (53 KB);
  `--check` uses lake probes and the net water area (28.19 sq deg, ~246,500 km2). Published by the coast-build
  workflow's dispatch with `set: lakes`. The frames (2-node fill) colour 99.68 % of that water (the rest: narrow
  channels and harbours GLWU does not model, e.g. the Bay of Quinte and the Thousand Islands, which stay uncoloured);
  the land set is unchanged. A build from the pinned GSHHG 2.3.7 archive is always held to the real checks (3 lakes,
  >= 100 islands, probes in both tiers, the area), whatever it produced (G33a P2-4). In winter the fill can colour up
  to ~3 km over an ice edge.
- **Fixtures**: `tests/capture_glwu_fixtures.py` (network + eccodes) writes `tests/fixtures/glwu/` (four steps' `.idx`
  lines, step 0's metadata with eccodes' coordinates at a few nodes, one bulletin) for `tests/model_frames/test_glwu.py`;
  `tests/fixtures/glwu/{latest.json, manifest.json, hs_f000.png, tp_f000.png, pdir_f000.png, f000_samples.json}` are
  the published run 2026102119's pointer, manifest, three step-0 frames and 60 node values decoded independently, for
  `tests/overlay/lake.test.js`.
- **The browser (overlay asset 2.21.0, the client core; the page flag, the credit, the readout text and the lake
  tables follow)**: under the GFS model's wave height and period the lake frame of the SAME VALID TIME rides beside the
  step's own frames as two more frame kinds in the scheduler (`'lake'`, `'lakedir'`; keys `glwu/<run>/...`, their own
  caches, one more download at once each, the same per-step wait cap as the pressure frame: a slow or missing lake frame
  never holds a step more than a second, the lake joins when it arrives, never an old lake under a new time). One
  `ModelGridLayer` draws both: the tiles of the lakes' box take their lake water coverage from `static/coast/lakes-v1`
  (`_lakeFor`, cached per tile like the land mask), `sampleLake` writes the lake frame's codes there (bilinear over the
  present nodes, not periodic, nothing beyond the grid), and `composeTile`'s alpha is the larger of the ocean fraction
  and the lake coverage; the readout and the particles' lattice use the same rule, so the readout is the drawn pixel
  across the shore and `lakeAt` tells the page where lake water shows nothing ("no data"). Contours read the composite
  lattice. The particles take the lake direction frame through `FlowAnimator.setLakeData` (the lattice reads the two
  lake frames per point, `lakeVec`; a lake point with no lake vector gets none). Under ECMWF only the mask
  applies: its own 0.25-degree lake cells show through (the GFS frames hold 0 over the lakes, so without a lake run the
  lakes are blank). A newer lake run noted by the run check takes over at the next step (its old fetches aborted, its
  caches afresh). Measured on the real frames: the lake pass 0.6-0.7 ms per tile with full coverage, the lake vectors
  ~4.5 ms per step when the whole grid is in view.
- **The page (overlay asset 2.21.1)**: env `GLWU_OVERLAY=1` hands the setting to the module (`var GLWU` in the gated
  block -> `create({glwu: GLWU})`; off by default, the flag-off page untouched); the stores exist only then. While a
  lake frame is drawn the map credit reads "... · Lakes: NOAA GLWU" (its title names the NDFD winds); a hover over lake
  water that shows nothing (past +149 h, no lake run, ice) reads "no data". The overlay's run line says "the forecast
  table is a NOAA GLWU (Great Lakes) run" while a lake station's table is on the page.
- **The lakes set's overview (overlay asset 2.21.2)**: the coast store fetches each set's own tier-0 overview beside its
  `index.json` (the land set `world-i.bin`, the Great Lakes set `lakes-i.bin`) and refuses an index that names another
  file. 2.21.1 asked for `world-i.bin` under `lakes-v1/` (404): no lake coverage, so the lakes were never drawn; the
  step-6 test site found it, and `tests/overlay/lake.test.js` now loads the published index and overview.
- **Lake water in all-land tiles (overlay asset 2.21.3)**: the land set counts the Great Lakes as land, so most tiles
  over the lakes are "nothing but land" there; the draw's all-land shortcut (G31-6) cleared them before the lake
  coverage was looked at (2.21.2 drew only the tiles that also held sea, e.g. Lake Ontario's). The coverage now
  comes first and the shortcut applies only without lake water; `composeTile` reads the all-land marker as alpha 0, so
  lake pixels take their coverage. Both the lake frames (GFS) and ECMWF's own lake cells were affected.
- **Only near the view (overlay asset 2.21.4, step 6b)**: the lake frames are fetched only while the lake grid's box
  lies within one screen of the view (`lakeNearView`, re-judged on moveend / zoomend / viewreset and a resize); away
  from the lakes nothing of them is downloaded, decoded or kept (the step-6 Hawaii cell had paid +9 MB and +72 ms/s of
  main thread for lake frames nobody saw), and the lake caches are emptied when the view leaves them. A pan onto the
  lakes during playback asks for the shown step's lake frames at once (the next steps wait for theirs up to the step
  cap). The particles' lake vectors are read per lattice point from the frames (no arrays over the lake grid: about
  3 MB less while the lakes are in view).
- **The G33 fix round (overlay asset 2.21.5)**:
  - **The view gate.** It compares the reach with the one it last acted on (`_lakeGateNear`), so a resolution change's
    step in the same zoom cannot hide the transition. It also acts while a step is still pending (an Update's first
    landing, the error panel): leaving the lakes aborts the lake fetches, empties both lake caches, and drops the drawn
    lake and the animator's lake data (`_dropLakes`).
  - **Lake runs.** The lake data carries its run, and the animator takes a lake direction only beside the height frame of
    the same run: after a newer lake run is adopted, the old run's lake stays drawn until the step lands, never mixed with
    the new run's directions. The store's 30-minute re-read keeps the run in hand usable (no blank lake on a Retry). A
    newer run it finds goes through the scheduler's one adoption path. An older pointer is ignored.
  - **Retries.** A lake run or lakes coast set that failed is asked again at a landing once its cooldown has passed. No
    lake frame is fetched while the coast set has failed (it could not be drawn).
  - **Calm water.** Under peak period, calm water reads "calm" on hover: the shown step's lake height there is present and
    under 0.2 m. That height frame is fetched on demand, one per hovered step, and never drawn. "No data" stays for
    everything else: past +149 h, no lake run, ice, and channels GLWU does not model (the Thousand Islands, the St Marys
    River, the Niagara River, the Keweenaw waterway).
  - **The credit.** "Lakes: NOAA GLWU" shows only while the lake water is on screen, not within the one-screen fetch
    margin.
  - **The tables.** A newest manifest without a bulletin list sends the tables to the kept runs.
- **The G33 re-check fix round (overlay assets 2.21.6 and 2.21.7)**:
  - **One adoption per lake run.** The store judges a re-read's answer against the run in hand when it answers, and
    never takes the run in hand a second time (a page left past a new lake run blinked its lakes and downloaded them
    again at the next field switch).
  - **Out of reach is out of reach.** Leaving the lakes drops every lake fetch, frame and drawn lake under any field or
    mode (wind and SST included), and the view gate's state is re-judged at once when a lake run loads.
  - **The hover.** The height frame re-shows the readout only while it is still the shown step's (no fetch chain during
    playback on a slow connection); one that lands after playback moved on shows the hovered step's word in place of
    "…" without asking for another frame (2.21.7); a height frame silent for 15 s reads "no data".
  - **The credit** counts lake water in the view (a tile's lake coverage in its visible part), not the lake grid's box
    (which holds most of Michigan's Lower Peninsula).
  - **Coast retries.** No lake frame is fetched while the lakes coast set loads again after a failure; its arrival starts
    the shown step's lake frames.
- **The lake tables** (UI asset 1.24.0): the 72 stations (`glwu_stations.json`: name, position and nearest civil zone,
  built by `tools/build_glwu_stations.py` from `glwu_fetch.STATIONS`, NDBC's station table and names written there for
  NOAA's 14 beach points) join the station list, the map and the picker after the GFS ones; they live in their OWN file,
  so `station_list.json` / `station_coords.json` / `station_timezones.json` and the forecast-point zone rule stay as
  reviewed. `resolve_model` makes a lake station's model `GLWU` (and never anywhere else); `parse_glwu` reads the
  bucket, never NOMADS: the pointer and manifest (5 min; a pointer that cannot be read keeps the last good run 6 h),
  the station's bulletin of the newest run that lists it, else of the kept runs before it (6, 12, 18 h back; the
  Cycle line names the run read: "Cycle : NOAA GLWU 20261021 19 UTC", the window says "GLWU · run ..."), and that run's
  `winds.json` for the Wind column. The text goes through the GFS bulletin parser itself (`_parse_bull_text`, the same
  layout: 150 hourly rows, directions turned TO -> FROM, the site-wide swell order). The cache keeps a lake table the
  full 30 min (a run's winds never heal; 45195 on Lake Champlain has none) and `wind_complete` is true for them.
- **Bulletin cycle hour** (job fix, 2026-10-22): NOAA writes the 01Z and 07Z cycles' hour space-padded ("Cycle :
  20261022  1 UTC"); the job's check wanted two digits and dropped every bulletin of those runs (run 2026102201 was
  published with none). `glwu.BULL_CYCLE_RE` takes one or two digits and compares the hour as a number.

### GFS station winds (plan section 53, the job)

The forecast table's Wind column is GFS's 10 m wind at the station: one quoted point line per hour in the station's
WAVEWATCH III spectra file. NOAA's AWS copy keeps those only inside the run's `gfswave.tHHz.spec_tar.gz` (about 1.7 GB,
a gzip of 918 members of 7.75 MB). The job `tools/model_frames/gfs_winds.py` (workflow `model-gfswinds.yml`, every 10
minutes and by dispatch, also through the trigger Worker) streams that archive once per run: gzip, then tar in stream
mode, one member in memory at a time. It keeps every step's point line with the site's own parse rule, so the values
equal what the site read from NOMADS's `.spec`.

- **Objects** under `gfswave/winds/v1/` (immutable but the pointer, which is written last):
  - `<RUN>/winds.bin`: count x 385 x 2 little-endian uint16, speed in 0.01 m/s and direction (FROM, degrees true) in
    0.1 deg; 65535 = missing. The `.spec` prints 2 and 1 decimals, so the values round-trip exactly.
  - `<RUN>/index.json`: `format` gfswinds-v1, `run`, `steps` 0..384, `stations` (id -> slot), `count`,
    `record_bytes` 1540, `missing`, `units`, `sha256` of winds.bin, `members`, `left_out`, `built_utc`, `source`.
  - `latest.json`: `run`, `published_utc`, `index`, `stations`, `steps` (max-age 300).
- **Discovery**: the last four 6-hour cycles, newest first, by a HEAD of each archive (at least 1 GB). NOAA writes the
  archive whole or not at all, about 5.5 hours after the cycle.
- **Exit codes**: 0 published or already live; 3 no complete run yet, NOAA not serving it, or the 20-minute build
  budget ran out (the next tick retries); 1 a failure (recorded under `failed/<RUN>.json`, retried after an hour, at
  most 6 times) or the stale alarm (the live run 18 h old or more, at most once every 6 h).
- **Checks**: every member a regular file `./gfswave.<id>.spec` of 1 B to 32 MB, once each, at least 500 members;
  at least 90 % of them start at the run's hour (another run's archive under this run's name is refused); the gzip
  stream is read to its end, so its CRC and length are checked. A changed layout fails loudly. A stalled download
  (60 s without a byte) is a failure; only the build budget is an exit 3. While a newer run cannot be built (not
  served, over budget, waiting after failures) the job still raises the stale alarm once the live run is 18 h old.
- **`--run RUN`** never moves the pointer back to an older run and never rewrites a published run.
- **Retention**: 8 runs (two days). The SWAN tables' wind back-fill reads the GFS run of the SWAN table's first hour
  (12Z of PacIOOS's publish day) for as long as that table is shown, about a day: just before PacIOOS's next update
  that is the 5th or 6th newest run, up to about 30 hours older than the newest. 8 leaves room for a late update.
- **Cost**: run 2026102512 on GitHub's runner took 67 s at a maximum RSS of 67 MB; it published 1,413,720 B for 918
  stations. Writes per run: 3.
- **Local**: `python tools/model_frames/gfs_winds.py --dry-run [--run RUN] [--out DIR]` builds without publishing and
  can write the two files to DIR; `--local DIR --run RUN` builds from a downloaded archive.

### NWPS forecast tables (plan section 59)

NOAA NWPS is the National Weather Service offices' nearshore SWAN model. Each office runs it 1-8 times a day (when
its forecasters start a run) to +144 h and publishes hourly 2-D wave spectra at named points on NOMADS
(`com/nwps/prod/<region>.<date>/<office>/<HH>/CG1/nwps.tHHz.spc2d_<id>_CG1.<office>.txt`). At 112 forecast
stations (`nwps_stations.json`: the office's point within 25 km of NDBC's buoy, the nearest office when two publish
it; built by `tools/build_nwps_stations.py`) the forecast window offers a model button **GFS | NWPS**; GFS-Wave opens
first.

- The job `tools/model_frames/nwps.py` (workflow `model-nwps.yml`, every 10 min, dispatched by the trigger Worker):
  per office the newest run whose CG1 directory is complete (the grib present, the newest file 5 min old) and newer
  than the live one; each station's spectrum read by `spc2d.py` and split into swells per hour by `partition2d.py`
  (the watershed of Hanson and Phillips; small parts merged; Tp from a log-parabola on the per-Hz spectrum; the
  energy-weighted mean FROM direction; ties after rounding each value's mantissa to 40 bits). Published to the frames
  bucket: `nwps/v1/<office>/<run>/<sid>.json`, the run's `index.json`, then `nwps/v1/latest.json` last; 4 runs kept per
  office. NOMADS is asked one request at a time; a throttle, a NOMADS that does not answer or the 20-min budget stops
  the tick (exit 3); a failed office build is retried after an hour, six times; a stale alarm after 24 h without a new
  run anywhere.
- The site (`parse_nwps` in app.py) reads only the bucket: the pointer (5 min; the last good one serves 6 h), the
  office's newest run listing the station, else its kept runs; each file checked. Rows in the GFS table's shape
  (feet, FROM, the site's swell rank); the Wind column is the GFS station winds (section 53). `?model=NWPS` is honoured
  only at the 112 stations; `/healthz` reports `nwps`.
- Limits (G41, step 8): the pointer is read by one thread at a time (a request holding a good pointer is served it
  meanwhile; one holding nothing waits up to 10 s); a failed read with nothing held is asked again only after a minute;
  every run name must be a real date; at most the newest 8 kept runs are tried, in 20 s in all (a read starts only with
  a whole read's time left), and none after the bucket stops answering (a missing or refused file, or one over its size
  cap, still moves on to the next run). The Location line names the NWPS point the numbers are for (the file's), not
  the GFS bulletin's point.
- An office that stops (owner, G41): the window says "· N h old" (header, in its own span so phones keep it, and the
  line above the table, also on the no-script page) once the run is over 36 h old (Honolulu's own runs reach ~32 h
  before the next lands). Past 72 h the station is served as one without NWPS: no NWPS button, and a request for NWPS
  gets the GFS table, until the office publishes again. The GFS path judges this from the pointer the server already
  holds (never a bucket read on a request's path); the live scheduler's pass (every 30 s) re-reads a pointer past its
  5-min TTL in a background thread, so the button follows the offices within about 6 minutes whether or not anyone opens
  an NWPS table, and a pointer whose last good read is over 6 h old decides nothing (G41 re-check F-1). The NWPS table's
  cache key carries the run the held pointer names, so a new run is served at once. One answer judges the model once.
  A run over 48 h old has early hours older than every GFS winds run kept, so its wind gaps there are final (the table
  kept the full 30 min; `wind_complete` true).
- Validation (2026-10-28, 30 stations, 4,350 hours): combined Hs vs NOAA's GFS bulletins |diff| 0.19 m; NOAA's swell
  systems of 0.5 m or more matched within 2 s / 30 deg 88 %; hour 0 vs the buoys |Hs - WVHT| 0.22 m. Record:
  `docs/reviews/nwps-G41-adversarial.md`.
- Rollback: `prod-pre-nwps-job` (the job), `prod-pre-nwps` (the site). Disabling `model-nwps.yml` stops new runs; the
  site keeps the last ones for the runs kept.

### PacIOOS SWAN tables: the Pacific islands and the freshness rule (plan section 64)

PacIOOS publishes a bulletin table for each of its SWAN nests' buoys in one format
(`https://www.pacioos.hawaii.edu/ssi/wavebuoy/swan_bull/tables/buoy.cdipNNN.table`: hourly rows of the total and six
partitions, the first 6 hours a spin-up the site drops). `SWAN_STATIONS` in app.py maps the NOAA station ids to the
table ids: the 12 Hawaii buoys and, since section 64, five Pacific-island buoys whose NOAA forecast station the site
already lists: 52200 Ipan and 52202 Ritidian Point (Guam), 52211 Tanapag (Saipan), 51209 Aunu'u (American Samoa) and
52212 Ngaraard (Palau, a 14-day run). The three Guam / Saipan buoys are NWPS stations too (office GUM), so their model
bar shows **GFS | SWAN | NWPS**. Left out: 51214 King-Poloa (American Samoa) and a second Palau point (cdip283): PacIOOS
has tables for them but NOAA no forecast station, so they cannot be a page station. The Wind column of a Pacific table is
the GFS station winds (section 53) joined by hour, back-filled from the earlier GFS run as for Hawaii.

PacIOOS runs once a day (~23:00 UTC) and skips some days; its whole table export stalled for three days once (section
58). The freshness rule is the NWPS rule of section 59: every table fetch records its region's publish time (the
table's `Last-Modified`), and the live scheduler's pass (every 30 s) asks one region's representative table for its
header (a HEAD, ~300 B) in a background thread whenever its record is over 5 min old (never on a request's path; a failed
probe is tried again after a minute; off with `SWAN_PROBE=0`). The regions share one run and one publish time per
region: `SWAN_REGION` = hawaii (cdip106), mariana (cdip121), samoa (cdip189), palau (cdip219). Past 36 h the window says
"· N h old" (the header's own span, the line above the table, the no-script meta line); past 72 h the station is served
as one without SWAN (no button, a request for SWAN gets the GFS table; the page's station list follows the answers in an
open tab) until PacIOOS publishes again. A record whose last good read is over 6 h old decides nothing (SWAN is offered;
the reader then judges the table by its own cycle line). The SWAN table's cache key carries the publish time the held
record names, so a new table is built at once. A table over 48 h old has early hours older than every GFS winds run
kept, so its blank wind cells are final (the table kept the full 30 min). `/healthz` reports `swan` (each region's
publish time, age and last error; never part of `ok`).

### Sea surface temperature tiles (plan section 44, the job)

A daily observation layer beside the model overlays: `tools/model_frames/sst.py` (GitHub Actions `model-sst.yml`, its own
concurrency group, cron hourly at :20, re-enabled by the keepalive; NOT dispatched by the 10-minute trigger Worker: a daily
product) publishes the newest NASA JPL MUR SST v4.1 day as tiles; `sst_tiles.py` holds the tiling (pure numpy).

- **Data**: MUR v4.1 (GHRSST L4, doi:10.5067/GHGMR-4FJ04): a gap-free daily analysis of satellite infrared and microwave
  data and in-situ measurements on a 0.01-degree grid (36,000 x 17,999), the night-time "foundation" temperature (below
  the daytime warm layer). Taken from NOAA CoastWatch's ERDDAP mirror (`erddap/files/jplMURSST41/`, anonymous HTTPS,
  NetCDF-4 read with h5py: ~750 MB a day; a day's file appears ~26 h after its nominal 09Z time; ERDDAP revises the last
  7 days daily, the job takes each new day once). Free and redistributable; credit "NASA JPL MUR SST v4.1 via NOAA
  CoastWatch ERDDAP" (the manifest's `source` block). Nothing free is global, cloud-free AND hourly at this resolution
  (hourly geostationary SST has cloud holes); the owner chose this daily analysis (2026-10-16).
- **Values**: open sea (MUR mask 1); MUR's own analysed values on land-flagged pixels within 3 pixels (3 km) of open sea
  (MUR extends its analysis smoothly over the coast: next to the sea those values differ from the sea pixel by 0.003-0.03
  deg C, like two sea pixels; 2026-10-14 file); then two rings of the mean of valued neighbours over other land pixels,
  so water the precise GSHHG coastline has inside MUR's coarser land mask gets a value (the client clips land with
  GSHHG). Lakes (mask 5, incl. the Great Lakes) and ice-covered water (mask 9 / 13: sea-ice fraction 15 % or more in
  every such pixel) stay empty, as the wave overlays leave ice empty. deg C = 25 + raw / 1000 (MUR stores kelvin as
  int16 x 0.001 + 298.15).
- **Tiers** (all on MUR's own 0.01-degree lattice, nothing resampled): `t0` the whole globe at 0.25 degree (the mean of
  the 25 x 25 pixels centred on each node: the overlay frames' 1440 x 721 grid; fixed code range -2..35 deg C);
  `t1` 0.05 degree (5 x 5 means) in 20-degree cells; `t2` every pixel in 5-degree cells. Cells are named by their
  south-west corner on multiples of the cell size from 0 (`<lat0>_<lon0>`, the client's `coastCellsForTile` rule:
  20-degree cells run -100..80) and hold n + 2 nodes a side, a one-node apron shared with the neighbours: row r / column
  c is centred at lat = lat0 + cell - r * res, lon = lon0 + (c - 1) * res (rows north to south). Codes are 8-bit, 0 = no
  value, v = lo + (code - 1) / 254 * (hi - lo) with lo / hi per cell (in the manifest, covering the apron so nothing
  clamps); steps were at most 0.07 deg C (t2) and 0.10 deg C (t1) on 2026-10-14. A t1 / t0 node with no valued pixel
  but land in its block takes the mean of its valued neighbour nodes (one ring).
- **Bucket**: `sst/v1/<YYYYMMDD>/<BUILD>/t0.png`, `t1/<cell>.png`, `t2/<cell>.png` (immutable; BUILD = the build's start,
  `YYYYMMDDTHHMMSSZ`: a tile's bytes depend on its cell's lo / hi in the manifest that names it, so a day built again never
  reuses a cached key), `sst/v1/<YYYYMMDD>/stats-` and `manifest-<published>.json` (the manifest's tier `dir` and `tier0.key`
  name the build directory), then `sst/v1/latest.json` (`{format, day, run, manifest, published_utc, complete}`, max-age
  300) LAST; `failed/<day>.json` (a failed build backs off 1 h, up to 6 times; pruned once older than every kept day);
  3 days kept (`--keep`), pruning whole `sst/v1/<8 digits>/` directories only. One day: 1,837 t2 cells (79.5 MB), 152 t1
  cells (5.1 MB), t0 221 KB; built in ~95 s here, 75 s on GitHub's runner (download 10 s, RSS 3.3 GB).
- **Download**: one whole request per try (ERDDAP answers every Range request on `/files/` with 416, so a cut transfer
  starts again from byte 0); 4 tries; the size must equal the listing's; given up after 40 minutes (a recorded failure with
  its back-off, well inside the workflow's 60).
- **Exit codes**: 0 published, nothing new, or a quiet outage (the ERDDAP listing fails, or a day keeps failing) while the
  LIVE day is fresh; 1 a download / build failure, or the live day more than 4 days old (on any quiet path; the run fails,
  GitHub mails, at most once every 12 h, record `sst/v1/stale-alarm.json`; nothing published yet counts as stale);
  2 bad arguments. `--dry-run`, `--local DIR` (write every object to a folder), `--file PATH --day YYYYMMDD` (a file on
  disk), `--force`; a run with `--day` never raises the stale alarm.
- **Accuracy** (2026-10-14 tiles against NDBC's own water temperature that day, daily means, US coasts and Hawaii):
  open-water buoys 129, median difference 0.24 deg C, RMS 0.58, 77 % within 0.5 and 91 % within 1 deg C (tiles 0.16
  warmer on average); coastal and harbour stations 128, median 0.48, RMS 1.01, 52 % within 0.5 and 80 % within 1 (tiles
  0.36 COLDER: shallow harbour water runs warmer than the foundation temperature; Honolulu Harbor 1.4, Kaneohe 1.0).
  Hawaii's seven buoys all within 0.5 deg C (the largest, Waimea 51201, +0.50). No value at the Great Lakes (lakes are left empty) or in rivers and back-bays
  beyond the 2-pixel fill. The coastal error goes BOTH ways: at open surf beaches the tiles can read warmer than the
  water (2026-10-15, through the site's own readout: all four Gold Coast / Tweed surf buoys 0.6-1.1 deg C warmer; G31),
  while harbours and bays read colder. MUR is a night-time "foundation" temperature: the afternoon surf zone can be
  warmer. The readout keeps one decimal (owner, 2026-10-16); the credit's title says the above.
- **Tests**: `tests/model_frames/test_sst.py` runs the same tiling code on a 20-pixels-per-degree synthetic world (the
  `Geo` lattice parameter). Review record: `docs/reviews/overlays-G31a-adversarial.md`.
- **The client store** (asset 2.18.0, plan section 44 step 3; nothing on the page yet): `static_overlay/overlay.js` reads the
  day through an `SstStore` (one per bucket root per page, like the coast stores): the pointer (`no-cache`, 15-s watchdog),
  the manifest (validated: the fixed -2..35 range, the tier geometry px x res = cell, a one-node apron, every cell a
  [lo, hi, bytes] triple), the 0.25-degree frame through the frames' own decoder, then cells on demand: a tile draws from
  the finest tier its zoom wants (the frame below tile zoom 5, 0.05-degree cells from 5, 0.01-degree cells from 8) whose
  cells are all in hand, the next coarser meanwhile, and is redrawn when they land; <= 4 cell downloads at once, a 24 MB
  LRU that never drops a cell a tile on the map could read, 404 / undecodable = final, other failures a 60-s cooldown; the
  pad cells one contour step around a tile are fetched too (never a condition for drawing). The store is a pluggable SOURCE
  on the tile layer (`setSource`): its sampler writes fractional codes over the fixed range into the layer's code buffer,
  bilinear over the four nodes around each pixel centre within its own cell (absent nodes drop out as in the frames), so
  the composer, the land clip, the readout (= the drawn pixel: the tile records the tier it drew) and the contours are the
  frames' code unchanged; the isotherms are traced on the unsmoothed values with the deg F offset carried as a level offset
  (hs / tp contours are byte-identical). A world copy samples the one world; a point on a cell edge reads the same from
  either side within the cells' quantisation (exactly when their ranges agree). `tests/overlay/sst.test.js` (14, incl. two
  published cells of 2026-10-15 against ERDDAP's own values and the real seam between them).
- **The overlay field** (asset 2.18.1, plan section 44 step 4): env `SST_OVERLAY=1` adds "Sea surface temperature" to the
  overlay dropdown (off by default: the client shipped first; a saved `sst` with the setting off starts Off). It is an
  OBSERVATION: while it is shown the model dropdown, the play button, the ribbon, the overview slider and the Run /
  Updated / Next Update lines go (the panel's mode `obs`); the head names the source ("NASA MUR (1 km, daily)"), the
  details show the scale and ONE date line ("Day: Thu, Oct. 15 (daily analysis)"; a warning past 4 days old, the job's own
  stale line); folded: "SST · Thu, Oct. 15". Units follow the site unit (deg F as an affine unit: the ticks 40-80 + "95+ °F",
  the isotherms every 2 F; Metric 0-25 + "35+ °C", every 1 C); the legend's knots give 15-30 C 56 % of the bar; the scale is
  the owner's pick B (2026-10-16), a thermal ramp from deep purple through blue, teal, green, yellow, orange and red to dark
  red, over the imagery at 0.75 opacity, clipped at the GSHHG coastline. No particles. The credit reads "Overlay: NASA MUR
  SST" (+ "· GSHHG"). A switch to it keeps the model's run, caches and time: back on a model field the picture returns at
  the time the viewer had (the same carry as a model switch) and resumes playing if it was; the observation saves no time.
  The 30-minute check only notes a newer day; the next switch to it shows the new one.
- **G31 fix round** (asset 2.18.3; record `docs/reviews/overlays-G31-adversarial.md`): the day's load times each step by
  stall (no answer or no bytes for 15 s), not one 15-s budget, so a slow link loads it; a viewer's pick, Retry or On tries
  again at once (the 60-s cooldown only paces automatic re-loads); each cell download has its own 15-s stall timer (then a
  cooldown and a retry); the tiles at the date line ask for the cells beyond it (their isotherms reach the 180 meridian);
  tiles entirely on land ask for no cells; one least-recently-used order across both tiers; leaving SST clears the decoded
  cells (owner; the day's frame and manifest stay); the isotherms every 2 C / 4 F below tile zoom 7 (owner), and the 0 C
  line is drawn; a time carried through SST keeps the model's run (a newer one stays an Update banner) and, after a reload,
  its play state; the stale line can appear on a tab left open on SST; the overlay select is named "Map overlay".
- **G31 re-check, fix round 2** (asset 2.18.4): a page loaded on SST keeps the saved play state (only a carried state writes
  it); Retry with nothing on the map and a newer run known lets a carried time's pinned run go (a run pruned while SST was
  up); bodies are read chunk by chunk wherever the browser streams them, so the stall timers measure silence on browsers
  without DecompressionStream too; a rejected `cancel()` is caught. `tests/overlay/sst_timers.test.js` (with `sst_net.js`: a
  fake clock and a held network of real PNG bytes) drives the real `decodeFrame` / `readBody` path.

## GFS bulletins and winds: where they come from (plan sections 40 and 53)

The forecast table of a GFS station is NOAA's GFS-Wave station bulletin (`gfswave.<id>.bull`) of the newest run.
NOMADS refused the production server's address on 2026-10-11 and every table said "No recent run found"; since then
the site reads NOAA's copy on AWS Open Data (`noaa-gfs-bdp-pds`, no per-address limit) first. From the 2026-10-22 18Z
run on, that copy keeps only run ARCHIVES under `gfs.YYYYMMDD/HH/wave/station/` (the per-station files answer 404), so
the bulletins come from the run's bulletin archive:

- **The archive**: `gfswave.tHHz.bull_tar`, 49,356,800 B, 918 members `./gfswave.<id>.bull` of 52,063 B, each behind
  a small pax header (access and change times only) and its own 512-byte ustar header. NOAA writes it whole or not at
  all, about a minute after NOMADS's files. Its layout has been the same in every run checked (member order, names,
  sizes, offsets).
- **The index**: `gfs_bull_index.json` (committed) gives each station's data offset and size; `gfs_bulletins.py` reads
  one bulletin with ONE byte-range request of `[offset - 512, offset + size)` and checks the member's ustar header (name,
  size, checksum: proof the layout still holds) and the text (Location = the station, Cycle = the run, at least 100 table
  rows). Build or check the index with `tools/build_bull_index.py RUN` / `--check RUN [RUN ...]` (the build streams one
  run's archive; the check reads six members of each run by range).
- **The order**: the archive first; NOMADS's per-station file second (at most 256 KB; the same station and run check)
  when the archive is missing, damaged or unreachable, or the station is not in the index. The error texts are unchanged
  ("Could not download .bull ..." when nothing answered, "No .bull file found ..." when a server answered without it).
- **The run probe** (cached 30 min, failures 2 min, one probe at a time: a request that comes meanwhile gets the run
  already held at once if it is younger than 12 hours; only a process that holds no run, or an older one, waits for
  the probe): the run's archive is asked (a HEAD) for each cycle, newest first (18/12/06/00Z today, then yesterday; a
  cycle younger than 3 hours is skipped: nothing is published that soon), until one is there; then NOMADS's 51201
  bulletin only for the cycles newer than that archive (NOMADS has a cycle a minute or two before AWS). Each HEAD has
  a 3 s connect / 5 s read timeout and is asked once more after a dropped connection or a 429, 500, 502, 503 or 504
  answer, while at least a second of the probe is left; no HEAD is started after 15 s. Only "not there" moves on to an
  older cycle: a 404 from the archive, a 403 or 404 from NOMADS (NOMADS answers 403 for a cycle it has not written
  yet). Any other answer (none, a refusal, another status, a 5xx again) stops asking that host in that probe, so a
  silent NOMADS costs one timeout and a hiccup on the newest archive never makes an older run look newest. An archive
  403 is a refusal too: NOAA's bucket answers 404 for a file that is not there. When a probe finds nothing, the last
  run found keeps being served (for up to 12 hours) and the probe is asked again after 2 minutes. An archive of
  another length than the index's still counts: its members are still read and checked one by one, and `/healthz` says
  so.
- **When NOAA changes the archive**: a member that moved or changed is refused by its header check and served from
  NOMADS; a station NOAA appended is not in the index and comes from NOMADS too. `/healthz` -> `gfs.bulletins`:
  `layout_suspect` is the last probe's verdict on the archive's size (cleared by a probe that sees the index's size),
  `probe_layouts` how many probes saw another size, `reads.layout` and `last_layout_read` (with its time) the members
  refused by their header. The repair: build the index from a new run, check it against a few others, run the tests,
  commit.
- **Dropped connections**: NOAA's bucket closes an idle keep-alive connection after about 5.6 s. A range read (or a
  probe HEAD, or a read of the winds table) that meets such a closed connection before any answer is asked once more
  on a new one, its connect and read timeouts clipped to what is left of the read's time limit (and only with at least
  a second of it left); timeouts and refusals are not repeated. A range answer must name the asked bytes in its
  `Content-Range`.
- **`/healthz` -> `gfs.bulletins`**: whether the archive and NOMADS are used, the index (members, archive size, a load
  error), the archive reads by outcome (`ok`, `unknown`, `missing`, `layout`, `content`, `transport`), the NOMADS
  fallbacks, the last error (a station id that is not in the index is counted under `unknown` but is not an error)
  and the last run served from the archive, plus the layout keys above. Reported only, never part of `ok`.
- **`/healthz` -> `gfs.run`**: the run served, whether the last probe failed (the run served is then the one kept
  from the last good probe, or none once that is older than 12 hours), `good_run`, and the ages of the last probe and of
  the last good one (`probe_age_s`, `good_age_s`; null before the first probe). Reported only.
- **The Wind column** (and the SWAN tables' wind back-fill): the GFS station winds job's table in the frames bucket
  first (`gfswave/winds/v1/<RUN>/index.json` + `winds.bin`, see "GFS station winds" under Model overlays; the bucket's
  address is the forecast points', from `MODEL_FRAMES_BASE` or `POINTS_ROOT`), NOMADS's per-station `.spec` second.
  A run's table is read once per process (two objects, ~1.4 MB) and the last 4 runs read are held (the newest and the
  SWAN back-fill's); it must be whole (its index's run, layout, slots and the sha256 of winds.bin), else NOMADS serves
  and the run is asked again after a minute (a 403 from the bucket counts as such an error). A run the job has not
  published yet (HTTP 404; about 10 to 20 minutes after the bulletin archive) is asked again after two minutes;
  meanwhile NOMADS's `.spec` serves as before. The values are the `.spec`'s own, so the table and the graphs are the
  same from either source.
- **`/healthz` -> `gfs.winds`**: whether the table is read, the runs held, and counts of stations served from it
  (`r2`), from NOMADS (`nomads`), stations not in a run (`not_in_run`), runs not published yet (`run_missing`),
  errors (`errors`, `last_error`) and the newest run served from the table (`last_run`). Reported only.
- **Diagnostics** (env): `BULL_ARCHIVE=0` reads NOMADS alone for the bulletins; `GFS_WINDS=0` reads NOMADS alone for
  the Wind column; `NOMADS_DISABLED=1` never asks NOMADS (the archive and the winds table only).

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

`station_list.json` (the station dropdown, the picker and the map's forecast markers via `/stations.json`) holds 723
points: 450 numbered buoys and 273 named points (more buoys and platforms such as the BSH, LF and TF sets, the forecast
offices' own points numbered below 51 such as HNL01 or MIA01, the OPC and TPC offshore points, the Pacific islands, the
African and Indian Ocean coastal points, and others). Since 2026-10-03 (owner) it leaves out the 3,302 BOUNDARY points
of regional and coastal models, which drew boxes around ocean areas on the map: the NWPS edges of each NWS office
(`NW-<office><n>`), the hurricane model's domain edges (`HWRF<basin>-<n>`), the NHC domain edges `RW-NH1`/`RW-NH2`,
other models' edges (`BKMG` Indonesia, `CDIP` Scripps' Southern California model, `KNY`, `MDG`, `SYC`) and the older
office sets numbered 51 and up (KEY, MIA, SJU, BER, MLB, PCB, SRH, CRP, LIX, TBW, JXFL, LCH, CHS, HGX, CCTX, MOB, BRO,
JAX, MNE, HNL; HNL51-68 are the boxes around Oahu and Kauai). `tests/test_station_list.py` holds the rule.
- Blank bulletins (plan section 52, owner 2026-10-24): NOAA also lists points inside sheltered water the 0.25-degree
  wave model has no sea for (Long Island Sound, Chesapeake Bay, the Potomac at Alexandria, the Strait of Juan de
  Fuca), whose bulletins have 385 rows with a literal 0 for the combined height and empty swell
  cells; the site showed a table of blank cells with only the wind column. `tools/build_station_blank.py` reads every
  listed station's bulletin of one run (NOAA's AWS tar of the run, `gfswave.tHHz.bull_tar`; else the per-station files,
  paced), records the blank ones in `station_blank.json`, and with `--apply` rewrites `station_list.json` without the
  ids blank in BOTH of the two newest recorded runs (one cycle's hiccup removes nothing; a missing bulletin never
  counts; the file keeps the newest four scans). A station that may be blank because of SEA ICE is never removed
  (owner, 2026-10-24: the model masks iced water, and the point carries waves again when the ice goes): a blank station
  poleward of 50 degrees, or in the Bohai / northern Yellow Sea, the Sea of Okhotsk / Tatar Strait / Hokkaido, the Gulf
  of St Lawrence / Newfoundland / Labrador or the Sea of Azov, is recorded under "ice" and kept (Alaska_NS3, 72 N, is
  blank under ice now). Removed on 2026-10-24 (runs 2026102406 + 2026102412): 44039, 44040, 44043, 44058, 44061,
  44062, 44063, 44064, 44072, 44087, 46088. An old link to a removed station still
  opens: `/api/forecast` answers the note "No model data at this point (sheltered water ...)" as final (no Retry), from
  the committed file, without asking NOAA. A visitor's stored default among them opens like no default (the page's head
  forgets it before the forecast module reads it, from `retired_stations` the server renders: the 11 ids) and says so
  once; a favorite the site no longer offers is dropped with a note. Every scan also reads the removed stations' bulletins (G35 F1): one with data
  again in both of the two newest scans leaves "removed" for "restore" in `station_blank.json`, is scanned every run
  while it waits there, and comes back at its old place with the next `--apply` (never on its own; the console names
  it; a plain scan never loses it: re-check R1-1); one still blank, absent or failed stays removed, and one waiting in
  "restore" that is blank in both newest scans again goes back to "removed". The removed station's answer carries its
  coordinates (`lat`, `lon`). The coordinates and zone tables stay whole (the point time-zone rule).
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
  zone: points never push a station's forecast out); two builds at a time, shared with the tide and wind windows (the
  build cap below), a third waits two seconds and is then told "The server is busy right now; try again in a moment"
  (`busy: true`, not kept): one deadline for the whole wait, and at once when both slots have been held longer than
  that (by any build: `SharedCap.stuck`); the forecast window asks a busy answer again quietly five times, 4 s apart,
  before it shows it with Retry (the point tool's first request likewise); a point's objects are fetched with ONE attempt (3 s to connect,
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

## Live buoys: background refresh (plan section 36, UI asset 1.17.1)

The "Live buoys" layer merges the agencies' station lists (ten at the time: NDBC, CDIP, QLD, AODN, AusWaves, Marine
Institute, CEFAS, SMHI, RWS, Copernicus; thirteen since plan section 63, which added NDBC's bulk report, GLOS and IOOS:
see "Live buoys: the US widen" below). Until this change the list route asked every agency whose list had expired INSIDE the visitor's
request and answered only when all of them had: every 30 minutes one visitor waited for a refresh, after a restart every
visitor waited for every feed, and one slow agency held everyone's markers (the Marine Institute's ERDDAP once timed out
three times: 124 s). Now nothing a visitor asks for waits for an agency.

The server (`buoy_sources.py`, `app.py`):
- Each provider keeps the list in hand and refreshes it in the background (stale-while-revalidate): due at 90 % of its
  list TTL, at the retry moment after a failure (300 s, also for a first failure) or at the full TTL. A failed refresh
  keeps the last good list for up to 6 h; an empty list after a good one counts as a failure. While the background
  service runs nobody waits for a provider's list: a caller asking a provider that has none yet gets an empty list at
  once and a refresh is queued (with the service off, the old rule: that caller fetches it, one fetch shared by every
  waiting caller). Positions that are not finite numbers are dropped (a NaN would make the whole list invalid JSON).
- One daemon thread, `live-scheduler`, starts with the FIRST REQUEST the process serves (Render's health check makes that
  the first seconds after a start), never at import: a gunicorn master that preloads the app forks its workers after the
  import, and a worker would inherit the locks the master's threads held but none of the threads. (Any forked child also
  resets every lock and flag of the service: `os.register_at_fork`.) Its first pass queues every agency, cheapest first
  (NDBC, NDBCW, CDIP, SMHI, MI-IE, CEFAS, GLOS, RWS, QLD, AusWaves, AODN, IOOS, CMEMS: `LIVE_WARM_ORDER`), then it
  loads the time-zone finder; after that it
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
  180 s, or three ticks if longer (`"stalled": true`); when every refresh worker has been held by one fetch for 15 minutes
  (`"fetch_stuck": true`: a slow-drip feed; the read timeout is per read, not overall). Those two are process faults a
  restart cures, which is what Render then does. Failed list builds in a row are reported (`"build_failing": true`) but
  never answered with a 503: their cause is in a feed's data, which a restart would meet again (it would only restart the
  whole site in a loop). Set it as Render's Health Check Path (Deploy to Render, step 6) on the production AND the test
  service, once the deployed code has the route.
- A buoy window opened from the remembered list while that buoy's agency is still loading after a start gets
  `503 {"retry": true}` with `Retry-After: 5` at once from `/api/buoys/<id>/latest` (no request waits for a provider's list);
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
| `LIVE_TICK_SEC` | `30` | Seconds between the scheduler's passes once every agency has answered (1-120; a bad value falls back to 30). |
| `LIVE_WARM_DEADLINE_SEC` | `10` | `/healthz` waits at most this long after a start for the full list (clamped to 0-14: Render cuts traffic after 15 s of failed checks). |
| `LIVE_STATIONS_EDGE_TTL` | `0` | Seconds Cloudflare may keep a complete list (`CDN-Cache-Control: max-age`); capped at 300; `0` = never (`no-store`). Partial answers are never stored. (Before section 36 the value `1` switched on a lifetime computed from the lists' ages; it now means one second.) |
| `LIVE_BREAK_PROVIDERS` | empty | Diagnosis only (test site): a comma list of agencies (e.g. `AODN,CMEMS`) whose list fetch fails on purpose, to watch the kept lists, the retries and `/healthz`. Never set it on production. |

Gunicorn: the service runs one scheduler per worker process; the site runs one worker (with threads), so each agency is
asked once per refresh. More workers would each run their own scheduler (correct, but more feed traffic).

## Live buoys: the US widen (plan section 63)

Three more feeds join the "Live buoys" layer (`buoy_sources.py`: `NdbcBulkProvider`, `GlosProvider`,
`IoosSensorsProvider`; listed in `app._build_buoy_providers`, refreshed by the background service like the others). Each
ranks below every national feed with spectra in the dedup order (`_station_priority`: NDBC 100, spectra 90 ... CDIP 55,
AODN 50, GLOS 49, IOOS 47, NDBCW 45, CMEMS 30), so a buoy two feeds list keeps the richer marker and names the other in
`also_sources`. Feeds that name buoys by NDBC / WMO number (NDBC, NDBCW, GLOS, IOOS) also merge two markers of the SAME
number up to 300 km apart (`merge_stations`' number tier): the IOOS ERDDAP's positions for NDBC buoys can be the corner of
a range, 133 km from NDBC's own (41056 on 2026-10-30). Measured in one in-process merge of all thirteen live feeds
(2026-10-30, with the wave-height check below): 567 markers before, 656 after (+89): GLOS 34, IOOS 45, NDBCW 17 kept;
IOOS's copies of 154 NDBC buoys hidden under them; six UK North Sea platforms now come from NDBC's hourly report instead
of Copernicus's copy.

A marker is listed only when its feed had a wave height recently (test-site finding V3-1: 16 of the first 101 new
markers opened an empty window, platforms whose wave sensor was off while their wind and temperature sensors reported).
GLOS and IOOS ask each station once, at list time, for its newest wave height (one ERDDAP query of under 100 bytes,
four at a time, `_wave_probes`): GLOS through its ERDDAP mirror (3 days back: the mirror lags the API by about a day;
a verdict is kept 6 h), IOOS on the sensors ERDDAP (4 days back) at every list refresh. The query asks ERDDAP to leave
out rows stamped more than an hour ahead and heights outside 0-30 m, so the newest row it gives is a sane one. "Nothing
matches" leaves the station out; ERDDAP's other 404s ("Currently unknown datasetID" while it reloads a dataset), failed
and unreadable answers, and an answer whose rows are all refused by the reading check, count as unknown and keep the
last verdict (never asked: listed). Every probe of a refresh shares one deadline (`PROBE_BATCH_S`, 150 s); a probe's own
download is capped (1 MB, 30 s). Stations the NDBC provider lists with spectra are not asked (NDBC's marker wins there).
GLOS believes a "no" only for a dataset its mirror lists (a sensor off while the platform reports; in November most of
them), however many platforms answer so. It judges nothing by its mirror when the mirror's newest dataset is older than
2 days (every activated platform listed, no probe asked); when the mirror cannot be read, the last good read (up to
6 h old) judges with the verdicts in hand and no probe is asked. IOOS's marker time is that newest wave height:
allDatasets' `maxTime` is the dataset's stated end, which can lag its rows by a day and counts every variable. Cost: the
GLOS and IOOS list refreshes took 45-51 s on the free test instance instead of 2-9 s (background runner only); about 50
small IOOS queries an hour and about 50 GLOS queries every 6 hours.

Every reading of the three feeds (and every probe row) passes one check, `_sane_reading` (G42): a row stamped more than
an hour ahead of the clock is left out, so is a height outside 0-30 m (fill values such as -9999 or 9.97e36 included);
periods outside 0-40 s, directions outside 0-360, spreads outside 0-180 and temperatures outside -5 to 40 C are blanked.
Downloads are read in chunks under a byte cap and a time limit (`_capped_fetch`: lists 180 s, windows 20 s, probes 30 s);
a body that trickles is cut at the limit from outside (each socket read gets the time left as its timeout, and a timer
shuts the socket), so no thread waits on one. A refresh that lists nothing after a good list keeps the markers AND the
station-to-dataset maps their windows need; once the keep window (6 h) ends and the empty list is published, the next
refresh lets the maps go too. GLOS's list needs its parameter map: without it the list refresh fails (keep-last) rather
than opening empty windows, and a window never fetches the 3.6 MB parameter list itself. CEFAS's Sandettie LV, Clipper
and Lomond carry NDBC's numbers in the merge (`_BUOY_ALIAS`, the page's `LINK_ALIAS`), and so do CMEMS's 7-digit WMO
ids (`*_TS_MO_6200NNN` = 62NNN, the page's rule), so their copies a few km away are one marker.

- **NDBCW: NDBC's bulk report.** `latest_obs.txt` (~100 KB, every 30 min) lists every NDBC station's newest observation;
  the NDBC provider lists only stations with a spectral file, so the stations that report waves WITHOUT one were left out
  (Canada's DFO buoys, Great Lakes partner buoys, CORMP, CBIBS, UK platforms ...). Ids `ndbcw:<id>`, shown by number like
  NDBC's; names from NDBC's `activestations.xml`. Height, dominant and mean period, the direction at the dominant period
  (NDBC's MWD) where measured (DFO's buoys have none), water temperature; a window's 24 hours from `realtime2/<id>.txt`
  (asked only for a listed station). Korea's KMA buoys (22101-22108) are left out, and so is any report with a 0-s period
  (their placeholder values: 0.5 m / 0 s). NOAA's data are public domain; the partner buoys NDBC relays (Environment and
  Climate Change Canada, the UK Met Office, Great Lakes partners) stay under their operators' terms.
- **GLOS: the Great Lakes Observing System's Seagull API** (anonymous JSON). The roster lists the platforms; those
  `activated` with a wave height are listed (seasonal: buoys come out of the water November-April); a platform silent for
  more than 4 days by GLOS's ERDDAP mirror is left out. The API names series by numeric parameter ids, mapped through its
  parameter list (read once a day); temperatures are stored in kelvin. A window's 24 hours are one `obs` request; the period
  and direction are filled from the nearest earlier reading within 30 minutes (GLOS stamps them minutes apart). "Free to use
  and redistribute; not intended for legal use" (the credit names GLOS and its partner buoys).
- **IOOS: the IOOS sensors ERDDAP** (Axiom Data Science, for U.S. IOOS): every regional association's wave datasets.
  Every hour `allDatasets` (positions and the newest time, last 2 days, ~1.8 MB) and the wave-height check above; the
  wave-height category index (each dataset's variables are in its summary, ~2 MB) is read every 6 hours and a failed
  read keeps the held one for up to a day. A dataset whose positions span more
  than 0.2 degrees (a glider, a
  Saildrone, a re-deployed buoy) is left out; datasets of the same buoy (a network's copy and the regional association's)
  are one marker, the newest wins. A window's 24 hours are one tabledap CSV naming only that dataset's own variables (ERDDAP
  refuses a query naming one it lacks), starting no later than the dataset's stated end (ERDDAP also refuses a time
  constraint past it although newer rows exist: gov-ndbc-46004 on 2026-10-30); rows whose QARTOD aggregate flag is 4 (failed) are dropped; swell / wind-sea
  partitions when the dataset has them. The window's credit names the dataset's operator (its `institution`); each
  dataset carries its operator's own licence text, so the label says "each operator's terms".

Known limits: NDBCW and most IOOS buoys give bulk parameters only (no spectra); GLOS Spotters' wind is an estimate from the
wave data (GLOS); Canada's DFO buoys report hourly without direction. A marker's grey "stale" look is set when the list
is built: IOOS every hour, from its newest wave height (grey 6 h after it); GLOS from its newest wave height at the ERDDAP
mirror, which runs about 30 h behind the API, so a GLOS marker turns grey after 2 days without a wave height there and
leaves the map after 3 (a stalled mirror gives no times: nothing greys); a quiet GLOS platform's window starts a day
before its last wave height.
Tests: `tests/test_buoys_widen.py` and `tests/test_buoys_widen_g42.py` (the review's fixes) on real answers
cut by `tests/capture_buoy_widen_fixtures.py`; the three fakes in `tests/fake_buoy_providers.py` are in the live-stations
golden (captured on the code before this section: the new code merges them identically).

## Tide stations (plan section 38, UI asset 1.18.6)

NOAA CO-OPS tide predictions (public domain; NOS asks for attribution, given in the map credits while the layer is on
and in the window) for the map's "Tide stations" layer.

The station list is a committed snapshot, `tide_stations.json` (3,501 stations: NOAA's `type=tidepredictions` list
joined with its `type=waterlevels` list for the `obs` flag, each station's nearest civil time zone by the live buoys'
rule, and for every subordinate station its reference station and NOAA's published time offsets). Rebuild it now and
then with `python tools/tides/fetch_stations.py` (`--offsets-cache FILE` keeps the ~2,250 per-station offset answers
between runs) and commit the file; the site never asks NOAA's metadata service at runtime. NOAA's metadata has a few
positions wrong (Niue, Neiafu and Nomuka in Tonga, San Lorenzo in Ecuador: a longitude's sign; Djakarta: the
latitude): `POSITION_FIX` in the tool puts them right and their zones follow; the tool stops (exit 2) when a derived
zone differs from NOAA's own `timezonecorr` by more than 3 hours at a station outside `KNOWN_ZONE_DIFF` (a dozen
places where NOAA's value is stale or on the other side of the date line), so a new wrong position is looked at before
it ships. Names NOAA writes in capitals ("HONOLULU", "PAGO PAGO Harbor") are written as names; capitals inside a
mixed-case name are abbreviations and stay ("Martha's Vineyard GPS Buoy", "PGA Boulevard Bridge"). `/api/tides/stations`
serves the page's copy (id, name, position, kind, zone, gauge) with an ETag and `Cache-Control: no-cache` (the browser
asks again on every page load and gets a 304 while it is unchanged; the edge keeps nothing), so a changed list shows on
the next page load after a deploy.

`tide_sources.py` reads the predictions (`TideService`, wired in `app.py` with the forecast points' bounded fetch:
one attempt, an 8 s wall-clock cap):
- A **harmonic** station ("R", 1,260) gets NOAA's own curve every 30 minutes plus its highs and lows.
- A **subordinate** station ("S", 2,241) has no curve at NOAA: only highs and lows, each its reference station's extreme
  moved by a published time offset and height ratio (checked at 25 of 25 stations, to the minute). Its curve is the
  reference station's NOAA curve re-timed and re-scaled between each pair of its own extremes (paired through the
  offsets), so it passes through every extreme exactly and keeps the shape of the real tide (stands, uneven rises).
  Measured where the truth is known (two harmonic stations, one shaped onto the other's extremes, the shaped stretches):
  San Diego 3 cm worst against a plain cosine's 31 cm, Seattle 5 (24), Galveston 5 (23), Nawiliwili 2 (17); a reference
  far away or in another tidal regime does worse (Anchorage shaped on Nikiski's curve, a 10.6 m range: up to 1.5 m).
  Where an extreme has no partner, or NOAA has no curve for the reference, that stretch is a half cosine between the
  extremes (up to ~20 cm off the real shape at a mixed tide); these are a few of the ~130 stretches of a station's
  32 days. A reference that is being fetched by another request, or cannot be fetched now, makes the subordinate
  station answer "busy" or a one-minute failure (the page asks again), never a cosine kept for the day. A pair of
  extremes of the same kind, or more than 20 h apart (diurnal Gulf tides reach 16.6 h), leaves a gap: nothing is invented.
- The window is 32 days from 00:00 UTC yesterday (every zone's "today from midnight" plus 30 days lies inside it; the
  curve carries 12 h more on each side, the extremes 24 h), kept per station per UTC day (predictions never change); a
  subordinate station shares its reference's cached curve. Only NOAA's "no predictions" answer is final, and only when
  NOAA gives it twice in a row (it has sent it for a moment for a station it serves: Nairai Island); a final answer is
  kept an hour on the server and 10 minutes in browsers, and so is an answer built around one (a curve without its list
  of highs and lows, or a subordinate's cosine because its reference has nothing: kept no longer than that reference's own
  entry, so the subordinate is shaped as soon as NOAA answers for the reference again). Any other NOAA message (a throttle, a
  hiccup), on either of a station's two requests, and any failure are remembered for a minute and asked again; one build per station at a time (per-key locks counted, so a waiter never orphans one), two
  build slots, "busy" instead of a long wait; LRU 256. A subordinate's payload names its reference (`ref`, `ref_name`).
- The sky travels with the payload, from `sky.events` (searched in 16-day chunks: it refuses more than 20 days at a
  time): the nights (last light to first light), the sunrise / sunset / moonrise / moonset instants (`events`) and the
  moon's phase, lit percentage and name every 6 h (`moon`; the page takes the sample nearest each local noon); about
  21 KB a station. Observed water level (`product=water_level`,
  6-minute samples of the last 48 h, of which the page draws today's part) is a separate route, cached 15 min, asked by
  the page only for stations with a gauge.
- Routes: `/api/tides/<id>` (30 min; an id not in the snapshot is a 404 without any upstream request: never a proxy),
  `/api/tides/<id>/observed` (5 min); busy or unreachable = 503 + `Retry-After: 5` + `retry: true` (the page asks
  again); the tide service is no live-buoy provider (not in the live list, the scheduler or `/healthz`).

The page (`templates/index.html`, `static_ui/tides.js`): markers (an empty div each, the sine-wave glyph is CSS) from
`TIDE_MIN_ZOOM = 9` (the legend gives no zoom hint at any zoom, owner 2026-10-10: its note shows only the list's
state, "loading…" or "unavailable"); the list is asked the first time the layer is on at that zoom. A click opens the tide window (`#tideWin`) on a DAY STRIP (after tide-forecast.com's
layout, owner 2026-10-08): one table whose label column stays put while the days scroll sideways; a column per local
day for 30 days from today's midnight in the display zone (the site's Time Zone when one is chosen, else the station's
own; the live-buoy panel keeps its own-zone rule); a closed day is 64 px (its date), an open day 240 px (the full date
over AM | PM halves); today opens by itself and a click on any day's header opens or closes it (several may be open).
Over the columns one SVG chart: the predicted curve with its fill, the nights shaded, a dot at every high and low with
the time and height written out on open days, the observed water level dashed, a red dashed line at the current time
with a dot at the current height and a level line to the axis (the current height in the label column; redrawn every
minute; a new day restarts the strip at today). Under the chart, aligned to the columns: HIGH and LOW (time + height),
Sun (rise / set), Moon (the phase glyph nearest the day's middle, mirrored south of the equator, and its set / rise).
Hovering or touching the chart reads the time and height under the pointer (a tap's readout ends with the tap; on a
strip whose zone changes name the readout names it). Within a day the time runs linearly over its own column from
midnight to the zone's noon over the left half and from noon to midnight over the right half, so 23- and 25-hour days
keep noon at the AM | PM line; where a clock goes back at midnight (Cuba) the day starts at the first 00:00. The drawn
curve is NOAA's 30-minute samples with the exact highs and lows put in between (so every dot sits on the line and the
current height and the readout follow the real peak; between samples the line is straight: up to about 0.4 ft off
NOAA's 6-minute predictions at Anchorage, on the steep rise after a low, a few hundredths of a foot at most stations); it is cut cleanly at the strip's two ends; a gap (NOAA's list of highs
and lows incomplete) is drawn as a gap and named in the notes. Heights above MLLW in the site's unit (axis labels in
feet or metres with up to two decimals). When the local day changes in an open window the strip restarts at today, its
notes follow and the station is asked for again (quietly; a failure keeps the strip and asks again every 10
minutes). A newer station voids an older
one's answers; a hidden (minimised) window builds its strip when expanded; Retry hands the focus back to the window;
text from the server is written as text only. For screen readers the table has a caption, its row labels are row
headers, and the sun / moon glyphs are read as words ("Sunrise 7:18 AM", the moon's phase and lit share).

Busy coasts (owner, 2026-10-08): each station gets a REVEAL ZOOM once per list, in order of importance (a station
with a gauge, a harmonic one, a subordinate one, then by id): the zoom from which it is 24 px clear of every more
important station already shown there (the grid of 32-px cells divides the world's width, so stations meet across the
date line). A station is drawn from its reveal zoom on, so an icon never vanishes as you zoom in, and no two icons are
closer than 24 px below the map's last zoom (11); the legend says nothing about the stations held back (owner,
2026-10-10: it is understood that more appear when zoomed in). At zoom 11 every station is drawn. The opened station is drawn whatever its reveal zoom, on top of the
other tide icons, and changes nothing else (no icon comes or goes when a station is opened or closed).

Clicks and taps: an icon is an 18-px square (the opened one 22 px). An icon whose centre lies in a more important
icon's box, or that has the other's centre in its own, is moved 12 px aside (east, west, south, north, then the corners;
24 px when none of these is free), its anchor and tooltip, not its place. So stations NOAA lists at one spot can each be
clicked (and, below zoom 11, the stations beside the opened one). The icons are placed in order of importance, the
opened one at its own rank with its larger box, so opening a station moves no other icon (the opened one steps aside
when its larger box needs the room); the moves are worked out again at every zoom. On a touch screen an icon's target
reaches 10 px beyond its box, cut to half the gap to the nearest drawn icon, so a tap on an icon's centre always opens
that icon's station (checked over the whole snapshot at zooms 9 to 11 with every station opened, also after zoom steps).

Keyboard: only the markers inside the view are Tab stops (a marker in the padding beyond it is not: Leaflet would pan
the map to it), a focused marker never pans the map, and a rebuild hands the focus to the same marker's new element
(to the map when the marker is no longer drawn: a zoom-out hid it). Enter or Space opens a focused tide (or live-buoy)
marker and its window's header takes the focus (after a Retry only when the focus is still in the window); closing a station
window (the x, Escape) hands the focus to the map when it was in the window. A failed station list is asked for again
after 30 s, not on every pan, and the legend says so meanwhile. The legend's notes wrap under their label: the legend
never changes width when one comes or goes.

Windows: a station chip gives its subtitle up before the station's name. An expanded station window may cover the
parked forecast chip's buttons on a wide screen (its title stays clickable); below 1180 px its default box rises above
that chip.

## Location aids (plan section 51, UI asset 1.25.3, overlay asset 2.22.3; names centred since UI 1.25.6, plan section 52)

Two rows in the settings gear, both on for a new visitor, each remembered in this browser (localStorage, "0" = off); the
lakes' shores under wind are always drawn (owner, 2026-10-23: no row):

| Row / layer | Key | Shows from | Source |
|---|---|---|---|
| Borders (country and state / province lines together) | `allshore.borders.v1` | country lines from map zoom 3, state lines from 5 (each line from its own Natural Earth zoom too) | Natural Earth admin-0 lines (disputed dashed) and admin-1 lines (dashed, fainter); public domain |
| City names | `allshore.places.v1` | each place's own zoom (Natural Earth `min_zoom`; GeoNames towns 8 / 9 / 10 by size) | Natural Earth populated places + GeoNames `cities1000` (CC BY 4.0) |
| Lake outlines (always, no row) | - | tile zoom 4, under the wind overlay only | GSHHG lakes of 100 km² or more (see Model overlays) |

`static_ui/places.js` (`window.AllshorePlaces.create(map, {base, storage, waitFor})`) draws the borders on 256-px canvas tiles in
the pane `placesLinePane` (z 340: above the particles, below the gridlines and the forecast points) as a light 1-px line
over a dark halo, and the names as text spans in `placesLabelPane` (z 620: above the station icons, below the tooltips; a
name takes no tap, so an icon under it stays clickable), each name centred on its place with no dot (a city is an area,
not a point; owner, plan section 52), thinned per view so no two overlap (largest places first; a name only where its
whole text lies on the map: one that would run off an edge is left out, never shifted; never
under the map's controls and their open panels, the overlay's sheet, the gridline labels, the map tools' fan and labels,
or the floating windows (the forecast, live-buoy, tide and wind windows, their bars and chips); at most 120, 60 on maps
under 600 px wide), measured in the font they are drawn in (a browser's minimum font size included), hidden while
zooming and rebuilt after a move or when any of those obstacles opens, closes, moves or changes size. The line layer is
on the map only while Borders is on, and a tile with no line keeps a 1x1 canvas. Nothing is fetched before the page's
load event (+ 1.5 s) and the overlay's first picture and its coastline (both drawn, in, failed or given up; at most 120 s), so
on a slow phone link the overlay's coastline is not starved; nor while both rows are off, or below zoom 3 (2 for
names). Every request asks for a low priority, and a download is given up only after 15 s with no byte arriving. The
data comes from `static/places/v2/` beside the model frames (derived in the overlay's gated block), or from env
`PLACES_BASE`: `index.json`, the world tier (`world-lines.bin`, `world-places.bin`, used to map zoom 7; only the file of
a row that is on is fetched, each drawn and credited as it lands, and one failing keeps the other) and 10-degree cells `c/<lat>_<lon>.l.bin` / `.p.bin` from zoom 8 (on demand, two at a
time, 6 MB of decoded data kept, never a cell the view uses); every file's length is checked against the index. The
credits "Borders: Natural Earth" and "Places: GeoNames CC BY 4.0" join the map's (i) while their rows are on and their
data has loaded. A wind station without a recent reading is drawn as a hollow grey ring, so it never reads as a town's
dot (G34). The overlay's coastline loader follows the same stall rule (15 s with no byte, not 15 s in all), and the first
picture of wave height, period or SST waits for it at most 15 s: on a slower link the field is drawn without coastline
clipping and a "Coastline still loading" note, and clipped the moment the coastline lands; its first frame downloads
beside the coastline and is given up only after 5 s with no byte (the owner's call after the G34 re-check; every frame
since 2.22.4). A row turned on while the other row's file is still loading starts its own file at once (UI 1.25.4), with a full 15 s for its first byte (UI 1.25.5).
Built and published by `tools/places/build_places.py` (coast-build workflow, `set: places`; `tools/places/README.md`).

## A live buoy on a forecast point (plan section 52, UI asset 1.25.9)

138 of the live buoys are forecast points too (the same NDBC id), many within a few hundred metres, and the live dot is
drawn over the forecast dot. The live window then offers **Forecast for this point** and the forecast window **Live
buoy**: the station with the same id (the live id without its source, or the id a small alias table gives a buoy
another network lists under its own number: CDIP 163 = NDBC 52201, the Marine Institute's M2..M6 = 62091..62095, CEFAS
8 / 10 / 305 (Sandettie Light Vessel, Clipper, Lomond) = 62304 / 62144 / 62146, CMEMS `*_TS_MO_6200NNN` = 62NNN) within
400 km, else the nearest within 1 km; a twin farther than 2 km (NOAA's forecast point
for a moved buoy lies 30-320 km from it) is named with its distance ("Forecast point 41041 · 19 mi away"; "Forecast
point · 19 mi" on screens 400 px wide or less; a screen reader hears "Forecast point 41041, 19 miles away"); none, no
button. A far live buoy is named as its window names it (its NDBC number, else its name: a CMEMS buoy "6200024", never
its file id). A window opened by the other's link links back to where the visitor came from (two buoys at one spot made
a round trip land on the other: G35 decision 3) for as long as that window stays open (a live window closed, or opened
from its own dot, has its own link again); closing a live window opened by "Live buoy" gives the focus back to that
button when it shows. A link to a station farther than 1 km moves the map there (decision
4). A removed station (section 52 C) has no dot, but its answer carries its position, so its sheltered note offers the
live buoy of the same id. On touch screens (`(any-pointer: coarse)`: any touch screen, whatever pointer is the primary
one; the rules follow the query when a pointer comes or goes) the forecast dots take taps 8 px beyond their edge (the
map's canvas renderer `tolerance`), and the renderer gives a tap to the NEAREST dot holding it (Leaflet's own canvas
gives it to the last drawn: a neighbour drawn later stole a tap on a dot's own centre; the `// ---- nearest path`
block, `tests/ui/nearest.test.js`); each live dot takes taps up to 8 px beyond its own, halved toward the nearest other
icon (live dots, tide icons, wind flags, My points, forecast dots; its own twin only while it lies within 3 px of the
dot's edge: a buoy's own wind flag keeps the dot as it was), one pass per round of rebuilds. The live dot is drawn ~7 px
to the right of its position (so the two dots can both be clicked with a mouse), so its twin forecast dot shows mostly
beside it; on a touch screen the live dot's pad takes that yellow dot too, and its forecast is one tap further, through
"Forecast for this point" (owner, 2026-10-25: kept as built). A tide icon's pad stops half-way to another station's wind
flag, as a live dot's does (a flag on its own gauge leaves it no pad). On a computer without a touch screen a mouse
hits as before; on one WITH a touch screen (many Windows laptops) the mouse gets the touch reach too (owner, 2026-10-25:
kept; the nearest icon still wins). The map tools take the tap's own position (Leaflet hands a click on a canvas dot the
dot's position). The link buttons are 44 px tall on touch screens. On phones the window opened or touched last is the one
on top; a link moves the focus into the window it opened, and Escape acts on the window holding the focus, else the one
in front, then wind, tide, live. The forecast window's notes (a removed default or favorite cleared, a point not kept)
sit between its toolbar and its table, so the table's scroll to the current hour never hides them; several are kept, one
per line; a removed default the page's head forgot is said at page start, whether or not the station list loads. The
wind legend's stall note is worked out again when the wind station list lands, and forgotten when the wind feed stops
(the layer unticked, the map below the stations' zoom). The station list is asked for once more only when its answer is
refused or broken; a fault in the code that uses it is logged. The block is `// ---- buoy links` in `templates/index.html`
(`tests/ui/buoylinks.test.js` runs it).

## Wind stations (plan section 39, UI asset 1.22.0)

Live wind readings on the map's "Wind stations" layer and a station's last 24 hours, from five public-domain NOAA / NWS
feeds (attribution requested, given in the map credits while the layer is on and in the window): NOAA NDBC buoys,
C-MAN and other fixed stations; the weather sensors of NOAA CO-OPS tide gauges; airports reporting METAR through
the NWS Aviation Weather Center (within 30 km of a coastline, or at sea: offshore platforms); and, for Hawaii (step 5c,
owner 2026-10-10 "Hawaii now"), the land weather stations the NWS API lists (api.weather.gov: HECO / HELCO / MECO, the
University of Hawaii mesonet, RAWS, CWOP, HADS and others, fed to the NWS through MADIS). iKitesurf's page was the
visual reference (owner, 2026-10-09); its data is WeatherFlow's and proprietary, so none of it is used.

The station list is a committed snapshot, `wind_stations.json` (~2,730 stations, 294 of them the NWS API's Hawaii stations:
125 utility, 68 mesonet, 67 RAWS, 28 CWOP, 6 HADS; ids `coops:1612340`, `ndbc:51003`, `metar:PHNL`,
`nws:001HE`; kinds gauge, buoy, cman, station, airport and, for the NWS API's stations, utility (HECO / HELCO / MECO),
mesonet (University of Hawaii, SCAN, CRN), raws, cwop, hads, weather; the nearest civil time zone; for a gauge the NDBC id
it is relayed under, its `alias`, and its weather sensors as letters, `wx`: a air temperature, w water temperature, p
pressure, h humidity). Rebuild it now and then with `python tools/wind/fetch_stations.py --sensors-cache
FILE --nws-cache FILE` (~10 min: one `sensors.json` per CO-OPS met station, NDBC's `latest_obs.txt` and station table,
the METAR cache and site list, the API for coastal sites not in the cache, the site's own GSHHG coastline for the 30-km
rule, the NWS API's station list per area (`--nws-areas`, default HI) and one observations probe per station, 0.75 s
apart) and commit the file. CO-OPS gauges are kept when their Wind sensor is active (232 of 315); NDBC stations when they
report a wind speed (moving ones left out: drifting buoys, ferries, ships, gliders; anchored lightships and a
station-keeping surface vehicle kept); NDBC's relays of kept gauges (224, named by the gauge's id in NDBC's station table)
become the gauge's alias, never a second flag; airports when they reported wind in the last 24 hours (1,777 incl. the
offshore platforms, counts of 2026-10-11); NWS API stations when a row of their newest 20 observations of the last 24 hours carries a wind
speed, leaving out the networks the other feeds cover (tide gauges, airports, NDBC buoys) and any station within 300 m
of one already kept. One station per spot (owner, G28): of stations within 100 m of each other the gauge stays, then an
airport's METAR, then NDBC's copy (the offshore platforms report both ways), and of two ids at one spot the one reporting
now. NDBC and METAR names go through the same case rules as the NWS names, plus compass points, distances and a US state
after a comma ("142NM NNE of Attu Is, AK").

`wind_sources.py` (wired in `app.py`; upstream requests through the forecast points' bounded fetch, with a User-Agent
naming the site as aviationweather.gov asks):
- The latest readings come from four `BuoyProvider`s in a list of their own (`get_wind_providers`): NDBC (one GET of
  `latest_obs.txt`, every 5 min; the file holds one line per station, its newest, and an hourly buoy's newest is often
  its wave-only line, so each station keeps its last wind reading until a newer one or 6 h: G28 B-4), METAR (one GET of the gzipped cache of the last ~80 minutes of reports, every 5 min;
  whole knots; a direction of 0 or `VRB` is no direction, 360 is north), CO-OPS (one `product=wind&date=latest` request
  per gauge, PACED: every minute the next `WIND_COOPS_PER_REFRESH` gauges in turn, `WIND_COOPS_WORKERS` at a time, so
  every gauge is asked about every 10 minutes; NOAA answered HTTP 403 to everything from the server for about a minute
  after 232 requests in a few seconds, and new tide windows failed then too. A gauge whose request fails keeps its last
  reading, only NOAA's "No data was found" drops it (any other NOAA message keeps it and counts as a failure: G28 A-4), a
  reading older than 6 h is dropped; every request of a slice failing is a failed fetch; the first 403 stops the rest of
  the slice before it is sent (those gauges go first after the pause: G28 A-2) and pauses the feed for 2 min with the
  readings kept), and the NWS API (one `observations?start=<2 h
  ago>&limit=6` request per station, `Accept: application/ld+json`, paced the same way: `WIND_NWS_PER_REFRESH` stations
  a minute, `WIND_NWS_WORKERS` at a time, so every station is asked about every 12 minutes; the newest row with a wind
  speed is the reading (the top of an hour is often a gust-only row); speeds converted from the row's unit code,
  MADIS-rejected values dropped, a calm row has no direction; an answer without a wind row keeps the station's reading
  until it is 6 h old; a 403 or 429 stops the slice and pauses the feed for 2 min), and **NOAA MADIS as the NWS
  stations' backup** (plan section 52, 2026-10-24: the NWS API's observations stopped updating nationwide on
  2026-10-22 13:20 UTC while MADIS, its upstream, kept the data; every NWS flag read "No recent reading"): one GET of
  MADIS's public surface viewer every 5 minutes (`WIND_MADIS_URL`, default the Hawaii query of
  `madisXmlPublicDir`; since plan section 55 every report of the ~300 Hawaii stations RECEIVED in the last hour
  (`timefilter=1`, `recwin=4`: ~290 KB; MADIS's 15-60 minute latency is caught, polls 5 minutes apart miss nothing),
  CSV in an HTML page, 4-40 s,
  its own fetch with a 45-s read timeout and 90 s for the whole answer), parsed by its header's column names (K to C, the altimeter in hPa, -99999
  missing, QC X / B dropped); the snapshot's NWS stations among them become relay entries (`alias_of: "nws:<id>"`), and
  the merged table takes the newer of a station's NWS API reading and its MADIS one (the gauges' NDBC-relay rule); a
  MADIS reading is kept 6 h. The table says which rows a relay gave (`fed_by`, counted in `/healthz` `wind.fed_by`) and
  each paced feed's and MADIS's newest reading ever (`newest`; `/healthz` adds MADIS's kept readings and the last
  answer's station and wind counts, and an answer with stations but no wind at all is logged); the legend notes "NWS feed stalled since <time>" when most NWS flags are
  grey (owner, 2026-10-24: fewer than half of the snapshot's NWS stations have a reading within the stale time, from the
  API or MADIS) and the NWS feed itself has brought nothing for 3 h, the time in the stations' own zone, named ("Oct 22,
  3:20 AM HST"); after a restart during the outage the feed's newest time is unknown and the note reads "NWS feed: no
  recent readings". A reading that stands alone in the window's charts (one MADIS report) is drawn as a dot. A station's
  window shows its MADIS readings of the last 24 hours (plan section 55, owner 2026-10-26: kept in the site server, no
  job): every reading of the snapshot's NWS stations the live polls bring is kept 25 h in compact arrays
  (`MadisSeries`, ~64 bytes a reading, a few MB in all); after a start the server asks the 24 hours it has not seen by
  OBSERVATION time, twelve 2-hour windows (`time=YYYYMMDD_HH00&minbck=-119`: the records observed in (T - 2 h, T]),
  newest first, one request a refresh, the next window 15 s after the last refresh ENDED (2 minutes after a failed
  window) while windows remain (the live poll keeps its 5-minute turn), so the 24 hours fill in about 12-15 minutes;
  MADIS answers a window only when its query is done (20-55 s), so the windows have their own fetch limits
  (`WIND_MADIS_WINDOW_TIMEOUT` 100 s a read, 120 s in all; on the test site 2026-10-26 windows were cut at the live
  poll's 45 s and saved only by the retry); the live poll has 75 s a read and 90 s in all (its query by receipt time
  took 45 s once). A window that fails, or answers with no station record or no wind reading (MADIS's known bad
  answers), is tried again, 3 times in all, then given up (its hours stay missing); a window given up while the live
  poll was failing too (MADIS down) is asked again once the live poll answers. A window's refresh is not the feed's
  refresh: it never clears the live poll's error in `/healthz` and publishes a new list version only when the list
  changed. Live polls more than 50 minutes apart (failures, or answers with no wind at all, seen 2026-10-24 and
  2026-10-26; also from the start, when the first polls fail) queue the hours between them again, from 2 hours before
  the last good poll (MADIS reports arrive up to ~70 minutes late). When the NWS API's request budget is used up, a
  station's window shows MADIS's readings and is asked again a minute later. The window's rows are
  the NWS API's and MADIS's, one per time, the NWS API's where both have one ("via" "madis" or "nws+madis"; the window
  says "Readings from NOAA MADIS (the NWS API has none in the last 24 hours)." or "Readings from the NWS API and NOAA
  MADIS."); while the back-fill runs the answer says `filling` (the window: "The earlier hours are still loading.") and
  is cached a minute. `/healthz` shows MADIS's `hist_stations`, `hist_rows`, `hist_oldest_age_s` and `backfill`
  ("n of 12", then "done"; `backfill_left`, `backfill_gave_up`, `backfill_lost`). Every NWS-network station's window carries NOAA's accuracy disclaimer
  for mesonet observations (`disclaimer`; MADIS's restrictions page asks for one). MADIS asks users to notify NOAA before placing its data on web pages: the
  providers used here (APRSWXNET / CWOP, RAWS, HADS, MesoWest incl. HECO, U-HAWAII) are its public, full-distribution
  category. They ride on the live-buoy
  scheduler's pass (`_wind_tick` after the buoy part; a failure there never reaches the buoys) with the same keep-last
  rule and fork reset but a refresh runner of their own (`WIND_REFRESH_WORKERS`, default 2: a slow or hung agency never
  holds one of the live buoys' workers, G28 A-5), and are NOT in the live-buoy list, memo, warm-up or golden;
  `/healthz` reports them under `wind` (with their runner and `fetch_stuck`), which is never part of `ok`.
- `/api/wind/latest` is the merged table the scheduler prebuilt (never a build or a wait in the request): `{now, stale_s,
  fields [id, t, s, g, d], rows, missing}` with speeds in m/s and directions the wind blows FROM; a gauge takes its NDBC
  relay's reading when it has no CO-OPS reading or the relay's is newer. Feeds still loading after a start are named in `X-Wind-Stations-Partial` with
  `Cache-Control: no-store` (the page asks again); else 2 min. `/api/wind/stations` serves the snapshot (ETag, `no-cache`: revalidated on every page load, like the tide list).
- `/api/wind/<id>/history` (60 s in the browser): the last 24 hours (`t, s, g, d` ascending, plus the weather the same reports hold
  where any row has it, step 5d: `at` / `wt` air and water temperature degC, `dp` dew point, `rh` humidity % (from the
  dew point where not reported), `p` pressure hPa, `pt` its 3-hour change, `vis` visibility km (with `vg` = 1 when it
  is "or more": the METAR API's "10+" / "6+", the latter its form of the international 9999, G28 A-3), `wx` weather words;
  and `conditions`, each field's newest value within 3 hours as `[t, value]` with the pressure `trend` over 3 hours)
  from NDBC's `realtime2` file (its ATMP / WTMP / DEWP / PRES / PTDY / VIS columns; cut at 24 h
  and 300 rows), CO-OPS `range=24`, the METAR API's `hours=24` behind a token bucket of 60 requests a minute (the API
  allows 100; temp / dewp / altim (else slp: one quantity per station, so the trend compares like with like, G28 A-8)
  / visib / wxString + cover), or the NWS API's `observations?start=<24 h
  ago>&limit=500` behind a bucket of 30 a minute (so the server sends the NWS API at most ~55 requests a minute with the
  feed's 25; temperature / dewpoint / relativeHumidity / seaLevelPressure or barometricPressure / visibility /
  textDescription by their unit codes); a CO-OPS gauge's weather comes from its own products (`air_temperature`,
  `water_temperature`, `air_pressure`, `humidity`, `range=24`: only those its `wx` letters list, asked together after
  the wind request under one 10-s deadline; a product that fails, is late or says anything but "no data" adds nothing);
  a CO-OPS message other than "No data was found" is a failure and the gauge falls back on its NDBC relay (`via:
  "ndbc"`). Request budgets (G28 A-1): every CO-OPS request made for a window comes out of ONE budget of 30 a minute,
  15 at once, shared with the tide windows (same agency, same address; the paced feed's 24 a minute come on top): the
  wind request takes its token first (none: busy, nothing sent); each of the gauge's weather products only if a token
  is free at once (one left out is like one past the deadline: the window shows the rest; G28 re-check R1-N2); NDBC
  `realtime2` files 30 a minute. Forecast points, tide windows and wind histories share one build cap of two
  (`_REQUEST_BUILDS`, G28 A-6): at most two builds hold request threads; a request waits for a slot at most 2 s, and
  answers busy at once while both slots have been held longer than that (`SharedCap.stuck`: a slow agency does not tie
  up the threads `/healthz` needs; G28 re-check R1-N4). The tide service's cache
  core: ok 10 min, a
  failure 1 min, busy never cached (503 + `Retry-After: 5` + `retry: true`), one build per station at a time, LRU 256;
  a gauge's window built without some of its weather products (left out by the budget, late or failed; NOAA's "no
  data" is an answer) is kept 1 min only, so the weather fills in at the next ask (fix round 7c verification).
  An id not in the snapshot is a 404 without any upstream request.
- Off with `WIND_STATIONS=0` (the routes answer 503, the scheduler leaves the feeds alone; the tests set it).

The page (`templates/index.html`, `static_ui/winds.js`): flags from `WIND_MIN_ZOOM = 9` (the legend gives no zoom hint
at any zoom, owner 2026-10-10: its note shows only the list's and the feed's state; the list is asked the first time
the layer is on at that zoom). A flag is a 40-px
box: a ring at the station, an arrow pointing the way the wind BLOWS, and the speed in the site's unit (mph / km/h)
14 px upwind so it never sits under the arrow; coloured by the band in whole knots (iKitesurf-style, owner): calm under
1 (a ring with "0"), light 1-9 blue, moderate 10-15 green, fresh 16-21 amber, strong 22-30 red, gale 31 and up purple;
a reading with no direction shows its number above the ring; no reading is a small grey ring; a reading older than 2 h
(the server's `stale_s`) is grey. Busy coasts are thinned as the tide icons are (a reveal zoom per station, 40 px clear
of every more important flag: a gauge or buoy before an airport, C-MAN or other NDBC station before the NWS API's land
stations, CWOP amateur stations last (owner, 2026-10-10); every station at zoom 13; the opened station always). The feed (`createWindFeed`) asks `/api/wind/latest` every 5 minutes while the
flags show (nothing while the tab is hidden; a partial answer again every 5 s, 24 times at most) and the drawn flags are
repainted IN PLACE (text, classes, the arrow's rotation; the markers, their listeners and the focus stay). Clicks reach
a flag only on its visible parts (the ring, the arrow's shape, the number: the 40-px box itself passes clicks to what lies
under it); on touch screens the ring has a disc of up to 36 px that ends 1 px before a visible dot beside it (a
forecast point or one of My points 12-26 px away keeps its own taps: G28 re-check R2-3), and a tide icon on the same
station (matched by id, not by
pixels) keeps only its own 18 px (the disc around it opens the wind window). A forecast point or one of My points close
enough to take a tap on the flag (8 px with a mouse, 12 px on a touch screen, while its layer is shown) leaves the ring
hollow and without clicks, and a click on the arrow or the number within that distance of the dot goes to the dot; a
SOLID ring, its arrow and its number always open the wind window (owner, G28: what you see is what you get). A pan or a
zoom redraws only the flags that change (the others keep their markers, listeners and focus). A click opens
the wind window (`#windWin`, a fourth floating window at the TOP-right corner below the legend, tools and gear, as tall
as its content, never over the parked chips nor lower than 42 px above the bottom edge, so it scrolls on a short screen;
its left edge right of the logo and the overlay panel (G28, owner). While the live-buoy or tide window is open its
right edge sits 60 px in from the margin, and where its own top does not show above one of them it is 30 px wider than
that window on the left: every open window keeps a strip to click, whichever is in front. On screens up to about
1,280 px wide that can reach over the overlay panel's right part. The page measures these boxes (`syncWindWidth`:
`--map-topleft-r`, `--ww-gap`, `--ww-min-w`). Its chip stacks on top of the station chips at the bottom-right through the
measured `--chips-h`, its phone bar above the tide bar and never capped; Escape closes it first) on the station's last 24 hours
(`createWindView`): an SVG chart of the speed (solid) and gusts (dashed), the site's unit on the left and knots on the
right, hour ticks every 3 h (6 h when narrow) with local midnights dated, gaps where readings are more than 90 minutes
apart, a row of direction arrows under it, a hover / touch readout (the nearest reading within 45 min), the current
reading with its age (every minute; the flag's own reading when it is newer than the history's last row, which the
browser keeps for a minute), a folded newest-first table of 48 rows, the zone the times are in and the source (the NWS
API's stations link to NOAA's readable time-series page); the readings are asked again every 5 minutes, quietly, but not
while the tab is hidden or the window parked (it catches up when shown); Retry keeps the flag's reading. On a narrow chart
(phones) the readout is short: the time, the speed with its compass point and gust, the air temperature. Under the wind reading a conditions line (step 5d: air / water
temperature, humidity, dew point, pressure with its trend, visibility, the weather in words, only what the station
reports, in the site's units: °F / inHg / miles or °C with one decimal / hPa / km; a METAR "6+" or "10+" visibility shows its "+") and under the arrows a small 24-hour chart of the air
(solid) and water (dashed) temperature on the same time axis; the Readings table gains Air / Water columns; the readout
names the air temperature. A station NDBC publishes no 24-hour file for (eight Korean buoys)
shows the flag's own latest reading and says so. Units and the display zone follow the gear (the site's Time Zone when
one is chosen, else the station's own). Limitations: airports report hourly (a flag up to ~90 min old at worst), buoys
hourly, gauges and C-MAN every 6-10 min; readings are the agencies' raw values; outside the US the layer is airports; a
wind flag beside a live-buoy dot or a tide icon is a second marker drawn below them (each stays clickable).

The map zooms to 18 since section 39 step 5b (owner: close enough to tell overlapping tide, wind, buoy and forecast
markers apart; Esri's imagery is sharp to 18-19 along most coasts). Where Esri has no tile it serves "Map data not yet
available" (one 2,521-byte JPEG): over the open ocean from about zoom 14, at a few remote coasts from 17-18, in the relief
from 15-17. `static_ui/basemap.js` (`AllshoreBasemap.tileLayer`, used for the imagery and, through the overlay, the relief)
looks every tile from zoom 13 up in the service's tilemap (`/MapServer/tilemap/z/top/left/32/32`, one request per 32 x 32
tiles, kept for the session; Esri sends `Access-Control-Allow-Origin: *`): a tile Esri has loads as usual, a missing one is
drawn from the closest coarser tile it has (its part scaled up into a canvas, then the tile's `src`); a tilemap that cannot
be read counts as "the tile exists". The station layers draw every station from zoom 11 (tides, `TIDE_ALL_ZOOM`) and 13
(wind, `WIND_ALL_ZOOM`); the relief is asked at zoom 16 at most (`WIND_LOOK.maxNativeZoom`, overlay asset 2.14.11).

| Variable | Default | Meaning |
|---|---|---|
| `WIND_STATIONS` | `1` | `0` turns the wind feeds and routes off. |
| `WIND_COOPS_WORKERS` | `2` | Gauges asked at once by the CO-OPS feed (1-8). |
| `WIND_COOPS_PER_REFRESH` | `24` | Gauges the CO-OPS feed asks per minute, in turn (1-500); 24 of 232 = every gauge about every 10 minutes. |
| `WIND_NWS_WORKERS` | `2` | Stations asked at once by the NWS API feed (1-8). |
| `WIND_REFRESH_WORKERS` | `2` | The wind feeds' own refresh threads (1-4), apart from the live buoys' `LIVE_REFRESH_WORKERS`. |
| `WIND_NWS_PER_REFRESH` | `25` | Stations the NWS API feed asks per minute, in turn (1-500); 25 of Hawaii's ~300 = every station about every 12 minutes. |
| `WIND_MADIS_URL` | the Hawaii query of MADIS's public viewer | The NWS stations' MADIS backup (plan section 52); empty = off. |
| `WIND_ECCC`, `WIND_JMA`, `WIND_IEM`, `WIND_SMHI`, `WIND_FMI`, `WIND_DMI`, `WIND_DWD`, `WIND_IPMA`, `WIND_MEI` | `1` | `0` turns that section-67 feed off (its flags then say "no reading"). |
| `WIND_ECCC_PER_REFRESH`, `WIND_IEM_PER_REFRESH`, `WIND_DWD_PER_REFRESH`, `WIND_MEI_PER_REFRESH` | 60, 8, 15, 4 | Files, networks or stations the paced feeds ask per refresh. |

### More countries (plan section 67, UI asset 1.27.0)

Nine more feeds, owner 2026-10-30 (all anonymous; each credited on the map while one of its flags is drawn in the view,
and in the window's source line with its licence wording): Environment and Climate Change Canada (MSC Datamart SWOB-ML,
ECCC Data Servers End-use Licence), the Japan Meteorological Agency (AMeDAS, Public Data License 1.0: "Source: Japan
Meteorological Agency website", and we say the data was edited), the Iowa Environmental Mesonet (state DOT road
weather networks and airport networks; "any lawful purpose"), SMHI, FMI, DWD and Met Eireann (CC BY 4.0), DMI (open
data) and IPMA (non-profit use with credit; IPMA asks for an e-mail notification to webmaster@ipma.pt, the owner's to
send). The stations are in the same snapshot, added by `python tools/wind/more_stations.py --cache DIR` after
`fetch_stations.py` (it keeps the other rows and rebuilds only its own; one-station answers are kept 12 h in DIR):
- kept when the station reports wind now, lies within 30 km of a coastline (the site's GSHHG tier-0 file) or of a large
  lake's shore (the lake-outline set, lakes of 100 km2 and more: the Great Lakes, Winnipeg, Ladoga, Biwa ...), or at sea;
- one per spot: never within 100 m of a kept station, within 500 m of a METAR site (an airport's own weather station is
  the airport) nor its METAR id (ECCC's NAV CANADA files, IEM's airport sites with K / P / T, DWD's ICAO), and DWD (coarse
  0.01-degree positions) not within 2 km of a kept station;
- DWD's own stations only (WMO block 10 = Germany): its station list also relays other countries' SYNOP reports, which
  are left out;
- kinds `met` (a national weather service's station, ranked with the airports where flags crowd) and `road` (IEM's
  `XX_RWIS`, ranked with the NWS networks); IEM's airports are `airport`; the IEM network is the new field `net`.

The build of 2026-10-30 added 1,824 stations (ECCC 312, JMA 580, IEM 534, SMHI 112, FMI 102, DMI 56, DWD 28, IPMA 87,
Met Eireann 13; the snapshot holds 4,548).

The feeds (`wind_more.py`, the parsers and providers; `wind_sources.make_providers` adds them; requests through the same
bounded fetch with the site's User-Agent):
- one answer a refresh: JMA (latest_time.txt, then that time's map file, 10 min; m/s and a 16-point direction, quality
  flags 0 / 1 kept; no gust in the map), SMHI (the latest hour of speed, direction and gust, 20 min; quality R left out),
  FMI (one WFS answer over Finland for the last 30 minutes, 10 min), DMI (the last 10 minutes of wind_speed, wind_dir and
  wind_max: the highest 3-second mean, 10 min), IPMA (observations.json, every station's last 24 hours, 30 min; an
  8-class direction);
- paced: IEM (currents.json per network, 8 networks every 2 minutes; knots), DWD (one poi CSV per station, 15 every 5
  minutes; km/h; the CSV's newest rows), Met Eireann (prodapi observations/<station>/today, 4 every 5 minutes; km/h,
  local time), ECCC (the latest folder's listing every 5 minutes, then only the files listed newer than the one last
  read, 60 at most; km/h; an element whose qa_summary is under 15 is left out: suppressed, missing, error, doubtful);
- each station keeps its newest reading until a newer one or 6 hours; a whole fetch that fails keeps the last list
  (keep-last); HTTP 403 / 429 pauses a paced feed 5 minutes.

A window's 24 hours (`/api/wind/<id>/history`, an explicit dispatch per source; an unknown source has none): from the
agency for JMA (the 3-hour point files, 10-minute rows with gusts), IEM (obhistory per UTC day), SMHI (latest-day per
parameter), FMI (one WFS answer), DMI (each parameter over the window) and DWD (the poi CSV), behind a budget of 30
requests a minute per agency (an empty budget answers "busy", asked again in a moment); from the feed's own memory for
ECCC, IPMA and Met Eireann (IPMA's feed brings 24 hours at once; ECCC and Met Eireann windows show the hours since the
server started, with the note "Earlier hours are not kept after a server restart."). Links in the window: JMA's AMeDAS
page, IEM's site page, Met Eireann's observations page.

## The station windows' refresh button (plan section 56, UI asset 1.26.4)

**The page.** Each of the three windows has a circular-arrow button before its minimise button (`#lwRefresh`,
`#twRefresh`, `#wwRefresh`; `.fw-btn.fw-refresh`), hidden in a chip or phone bar. `createStationWindow`
(`static_ui/forecast.js`) runs it: one refresh at a time (a click while one runs is ignored), `aria-busy` while it runs
(the arrow turns; dimmed instead under `prefers-reduced-motion`), and after a failure the button is orange with the title
"Could not refresh. Try again." (also said in a polite status line) until a load succeeds. The old data stays on screen
while the new data loads and when a refresh fails; nothing else in the window moves:

| Window | What a refresh asks | What it keeps |
|---|---|---|
| Live buoy | NDBC: the wave summary and the spectra; others: the latest reading and (AODN) the spectra, fetched before drawing | the body's scroll; the whole view when the spectra it shows cannot be refreshed (the button then says so) |
| Tide | the predictions revalidated (the same all day: a 304) and the water level asked again | the opened days, the strip's sideways scroll |
| Wind | the station's 24 hours | the open Readings list, the scroll |

A window still showing its spinner loads as usual; a window showing an error reloads (the Retry; the focus stays on
the button). A new station or a close drops a refresh still on its way (no verdict). A buoy refresh that brings back
everything but the spectra the window shows keeps the whole view and reports a failure (G39 V3-1).
The "15 minutes" is the data's age when the tab is shown again, not how long the tab was hidden. Most non-NDBC buoys
(CDIP, QLD, AusWaves, the European feeds) are shown from their feed's own copy, refreshed every 5-30 minutes: for them
the button only replaces the browser's copy; NDBC buoys, AODN's spectra and (plan section 63) the NDBC bulk-report,
GLOS and IOOS buoys' 24 hours are asked of the agency again. The live and tide windows remember when their data last loaded: an
open, expanded window whose data is 15 minutes old or more refreshes once when its tab is shown again or it is expanded
from its chip (`STATION_STALE_MS`). The wind window already asks again every 5 minutes by itself (and when shown), so
only its button is new.

**The server.** The live-buoy, wind and tide windows ask again for their station's newest data when the refresh button in their header
is pressed. The page asks the SAME URL with the request header `X-Allshore-Refresh: 1` (and `cache: 'reload'`, so the
browser's copy is replaced). On the server (`app.py` `_refresh_s`, `REFRESH_MIN_AGE_S` 60) a per-station answer cached at
least a minute ago counts as missing and is built again from the agency; a younger one is served, so a button pressed
again and again asks an agency at most once a minute per station. A refresh that fails (the agency down) leaves the good
answer in place for everyone and counts toward the minute; a "busy" answer (a request budget used up, every build slot
held) is not counted and replaces nothing. A refresh never makes an answer worse: a rebuilt wind history or water level
whose newest reading is older than the one held (e.g. NDBC's relay of a CO-OPS gauge while CO-OPS is down), or as new
but only partial (`"partial": true`: an agency budget used up, weather products missing), leaves the held answer in place
(`_held_is_better`, G39 V3-2 / G39-3). An AODN spectra refresh (8 THREDDS requests, 13-15 s) takes the one slot of
`_SPECTRA_REFRESH` without waiting, else answers 503 busy with `retry` (the button turns orange): refreshes of several
buoys never hold every request thread (G39-1). The singleflight per key, the shared build cap, every agency budget and the
routes' cache headers are unchanged; without the header nothing changes. Where it applies:

| Route | Cache it refreshes |
|---|---|
| `/api/wind/<id>/history` | `WindHistory` (10 min; an NWS station costs one NWS API request at most: MADIS's 24 hours are in memory) |
| `/api/tides/<id>/observed` | `TideService.observed` (15 min); the tide predictions are the same all day and are only revalidated |
| `/api/ndbc/station/<id>/components` | `NDBC_COMPONENT_CACHE` (30 min); `/wave-summary` reads NDBC on every request already |
| `/api/buoys/<id>/components` | `AODNProvider.spectrum` (20 min); `/api/buoys/<id>/latest` stays the feed's copy (refreshed every 5-30 min) |
| `/api/buoys/<id>/latest` (NDBCW, GLOS, IOOS) | the provider's 24-hour answer (5 min, `_DetailCache`); one request to NDBC / GLOS / IOOS's ERDDAP |

## The TLS trust list (plan section 57)

In October 2026 eMudhra disclosed that the keys of eight of its intermediate certificates (and their cross-signs) were
stolen; a client that still trusts them accepts a forged certificate for any host. Python's TLS does not check
revocation, so the protection is the trust list itself:

- **The site.** `requirements.txt` pins `certifi>=2026.10.23` (certifi 2026.10.18 and later leave the compromised
  certificates out; 2026.10.23 keeps only "emSign Root CA - G1", which none of them chains to), so every Render build
  installs at least that release; requests verifies against it. Section 47's extra copy of that root for the SWAN
  download (`certs/`, `_ca_bundle()`) was removed: PacIOOS now serves a Let's Encrypt chain. `/healthz` -> `tls`
  reports the installed `certifi`, whether a request verifies against certifi's file once requests has applied the
  environment (`bundle_is_certifi`: false when `REQUESTS_CA_BUNDLE` or `CURL_CA_BUNDLE` points elsewhere) and which of
  those two and `SSL_CERT_FILE` are set on the host (`env_override`; requests ignores `SSL_CERT_FILE`); never part of
  `ok`.
- **The publishers.** The six micromamba workflows pin `certifi` and `ca-certificates` (urllib's downloads use conda's
  OpenSSL list, the R2 uploads botocore's) and list both in the step summary. coast-build's pip jobs install certifi so
  botocore uses it; its `curl` downloads pass certifi's file and an empty CA folder (`--cacert "$CA" --capath
  "$NOCAPATH"`): on 2026-10-27 the Ubuntu runners' own list still trusted one of the two test hosts (the GeoNames file
  has no pinned digest), and Ubuntu's curl keeps reading its CA folder `/etc/ssl/certs` when only `--cacert` is given
  (the dry run saw `--cacert` alone accept that host). CI's Node 24 takes the newest release (`check-latest`; 24.22.1
  seen).
- **Checking it.** `python tools/tls_check.py [--real] [--paths ...]` connects to Render's two test hosts
  (`dv-g3b-emudhra.we-have-a-problem.com`, `dv-g3b-aaa-emudhra.we-have-a-problem.com`; both must be refused) through
  certifi, the system default and botocore's CA file (`--paths` names the ones an environment uses: the site uses
  requests only, i.e. `certifi`); `--real` also checks that the hosts the site and the jobs read (coast-build's
  download hosts included) verify. A test host passes only when its issuer is not trusted (OpenSSL codes 2, 19, 20,
  21): an expired or wrong-host certificate proves nothing (both test certificates expire on 2027-05-03) and fails.
  Only `dv-g3b-emudhra` tells an old certifi from a new one; `dv-g3b-aaa-emudhra` (Comodo's "AAA Certificate
  Services" cross-sign) is refused by older certifi releases too and matters for system lists. A real host that cannot
  be reached at all (asked twice) is reported as unreachable, not failed: a network result; a TLS answer that is not a
  verified handshake fails. The R2 upload host is not in the list (its name holds the account id): the first publish
  after a change proves it. The site itself makes no HTTPS request outside requests (no urllib / ssl use), so the
  system list of Render's image does not decide what it trusts.
- **Known limitation: the installers.** pip (on Render and on GitHub's runners) trusts the machine's own list plus
  the certifi copy bundled with pip, and micromamba most likely the runner's list: on 2026-10-27 both still trusted a
  compromised chain, and no install is hash-pinned. Exploiting it needs the stolen keys and a position on the network
  between Render or GitHub and the package servers; it existed before this change and is accepted for now (owner,
  2026-10-27). The full fix is hash-locked installs (a section of its own if wanted).
- A future CA incident: raise the floor in `requirements.txt` and the pins in the workflows together (the pin tests in
  `tests/model_frames/test_tls_pins.py` and `tests/test_swan_cert.py` name the release). model-frames-tests runs
  `test_tls_pins.py` when a workflow, the check tool or the tests change; `tests/test_swan_cert.py` (the site's floor,
  no other trust list in any site module or start file, `/healthz`) runs with the site's suite, which no workflow runs.
