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
is not extra detail.

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
Coastline clip (asset 2.6.6): wave height and peak period are clipped to the ocean in the browser, so the field
stops exactly at the coastline; wind is never clipped. Every map tile's land alpha is rasterised once per tile per
zoom from GSHHG polygons served beside the frames under `static/coast/v1/` (tier 0 `world-i.bin`, ~1 km, for
tile zoom <= 6; tier 1 `f/<lat>_<lon>.bin`, full resolution, one 5-degree cell per file, fetched for the cells in
view at zoom >= 7, <= 32 MB of decoded chunks kept). The hover/long-press readout consults the same mask and shows
nothing over land. If the coast data cannot be loaded the field is drawn unclipped with a warning in the panel.
See tools/coast/README.md for the builder, the publish workflow and the LGPL notice. Review records: `docs/reviews/overlays-G*-adversarial.md`. Attribution on the map: "Overlay: NOAA GFS-Wave/GFS"; the panel carries the full
sentence ("Source: NOAA/NCEP GFS-Wave (WAVEWATCH III) and GFS via NOAA Open Data Dissemination; rendered by
Allshore Surf. Not an official NWS product.").
