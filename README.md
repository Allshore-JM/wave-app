# NOAA GFS Wave Table View Web App

This repository contains a Flask web application that fetches the latest NOAA GFS wave `.bull` file for any available buoy station, parses the data, and displays it as a multi-level **Table View**. Everything that stays locked while the body scrolls lives in a sticky `<thead>`: three info rows (Cycle, Location, and Time Zone) followed by two column-header rows (the swell-group names, then the per-swell `Hs`/`Tp`/`Dir` parameters with their units). The parsed forecast values follow in `<tbody>`.

## Features

- **Buoy Dropdown**: The home page includes a dropdown menu preloaded with all NOAA GFS stations available in the `.bull` directory. You can select any buoy station to view the latest model data.
- **Automatic Run Detection**: The application automatically determines the most recent model run (18z, 12z, 06z, or 00z) by probing the NOAA directory structure for the selected date and hour. If the latest run isn't available, it falls back to earlier runs.
- **Multi-Level Forecast Table**: The parsed data is rendered as a single HTML table built by `build_html_table` in `app.py`. The sticky `<thead>` holds three info rows (Cycle, Location, Time Zone) and two column-header rows (swell-group names, then the `Hs`/`Tp`/`Dir` parameters and units), and the forecast rows live in `<tbody>` so the body scrolls beneath the frozen header.
- **Deployment-Ready**: The repository includes a `requirements.txt` file and a `README.md` with instructions for deploying the web app on [Render](https://render.com) or running locally.

## Getting Started

### Local Development

To run the app locally, first install the dependencies and then run the Flask server:

```bash
pip install -r requirements.txt
python app.py
```

The app will be available at `http://127.0.0.1:5000` in your browser. From there, select a buoy station to load the latest wave forecast and view it in table format.

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

- `app.py` — The main Flask application. It includes the route for the home page (plus JSON endpoints for the station list and live NDBC buoy data), the logic to detect the latest model run, fetch `.bull` files, parse them, and format the output.
- `requirements.txt` — Lists the Python dependencies needed to run the app (`Flask`, `requests`, `gunicorn`, `pytz`, `timezonefinder`, `beautifulsoup4`).
- `templates/index.html` — Jinja2 template containing the HTML structure for the home page. It uses Bootstrap for styling and includes a buoy selection form and the forecast table display.
- `README.md` — This file. Provides setup instructions and describes the features of the project.

## Contributing

Contributions are welcome! If you want to add new features, improve the parsing logic, or update the UI, please submit a pull request. For major changes, please open an issue first to discuss what you would like to change.
