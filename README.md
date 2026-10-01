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
`sessionStorage 'chartRange'` as before. The table is the compact form (short dates with the full date on hover, no info
rows: the window shows Cycle / Location / Time Zone in one line above it), about 1,000 px wide for its 23 columns. The
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
    than 50 km away). Dashed rings every 1,000 nm (2,000 km in Metric) out to most rays' reach.
  - **Readout:** pointing at the map (off the fan) shows the bearing and distance from the spot, whether that water's
    swell can reach the spot (in the window, partly shadowed, blocked by land N miles from the spot, beyond the polar limit), and
    deep-water travel times at 14 and 18 s (in hours under a day), with a dashed line along the great circle to the cursor.
    On a touch screen, Lock and tap: the tapped readout stays until the next tap (also when a window opens over the
    map), and a tap's compatibility mouse events never start or move it.
  - **Lock** keeps the window on the map and gives the map back to the page: forecast points, live buoys and
    double-click zoom work again, and a click only moves the readout. Escape then acts only with focus in the bar;
    Clear, closing or another tool unlocks.

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
`npx wrangler deploy`) calls the workflow-dispatch API at :05, :15, ... UTC; GitHub's own schedule stays on as a
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
