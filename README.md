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
- `templates/index.html` — Jinja2 template containing the HTML structure for the home page. It uses Bootstrap for styling and includes a buoy selection form, table display, and download link.
- `README.md` — This file. Provides setup instructions and describes the features of the project.

## Contributing

Contributions are welcome! If you want to add new features, improve the parsing logic, or update the UI, please submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

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
a new `PREFIX` version instead. The publisher pins its conda packages and action commits and lists the
installed versions in the step summary; bump the pins deliberately. External cron trigger: GitHub drops or delays
most `schedule` ticks, so a Cloudflare Worker (`tools/model_frames/trigger/`, deployed by Cloudflare Workers Builds
from this repository -- root directory `tools/model_frames/trigger`, build command `npm run build`, deploy command
`npx wrangler deploy`) calls the workflow-dispatch API at :05, :15, ... UTC; GitHub's own schedule stays on as a
fallback and a duplicate run short-circuits. The Worker needs the secret `GITHUB_TOKEN`, a fine-grained token for
this repository with "Actions: read and write", entered under the Worker's Settings > Variables and Secrets (rotate
it there; it is never in the repository). Runs it starts show the event `workflow_dispatch` with the token's owner
as actor. To stop it, disable the cron under the Worker's Settings > Triggers (or delete the Worker); a full stop
also disables both workflows as above. Data policy in the browser: the 0.5 degree frames are used below zoom 3.5 on desktops, below zoom 7
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
Wave-height colours and contours (asset 2.7.6): wave height uses value knots so 0-3 m fills about half of the
legend (owner-picked spectral palette; the knots stretch to whatever legend the manifest carries); tp and wind keep
linear scales. A Contours checkbox (wave height and period, off by default, remembered for the tab) draws light
anti-aliased lines every 2 ft / 0.5 m and every 2 s (doubled below zoom 4), about 1.8 px wide at zoom 8 and closer,
thinning to about 1 px at zoom 3 and below where they are densest. The lines are traced on a lightly
smoothed copy of the frame ([1,2,1] over the model nodes, each node held within half an 8-bit step of its own value), so
they do not follow the 8-bit terraces of flat seas and still sit where the colours and the hover readout put their
level (next to islands as in open water); the colours, transparency, the coast clip and the readout keep the raw
values. No line beside missing data,
where lines would crowd under 3 px apart, along the flat foot of a steep ramp, or in a model cell where the peak
period jumps more than 2 s between neighbouring nodes (the smoothing never blends two swell regimes).
Animation (asset 2.9.2): an Animation checkbox on the Opacity row (off by default, remembered for the tab) draws
particles with fading trails that flow along the dominant swell direction under wave height and peak period, and along
the wind under wind speed, on a canvas of their own under the forecast points (no pointer events; markers keep their
taps). The directions come from the `pdir` / `wdir` frames of the same step, loaded through the frame scheduler
beside the field frame (the target's field frame first, then its direction, then the ring of frames around it, never more
than two downloads at once; `pdir` at the 0.5 degree frames below zoom 7 and the 0.25 degree ones from 7.5, `wdir` at 0.5
degrees only). A direction is shown only for the step on the map: a step change clears the animation until that step's
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
