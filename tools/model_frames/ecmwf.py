"""ECMWF overlay frame job (plan section 43): render + publish the newest COMPLETE ECMWF open-data run.

python tools/model_frames/ecmwf.py          # env and options as tools/model_frames/run.py

The same pipeline, encoding, coastal fill, guards and retention as the GFS job (run.main with this Source), from
ECMWF's free real-time open data (CC BY 4.0), IFS 0.25 deg, 00Z and 12Z cycles to +360 h (85 frames), published
under ecmwf/0p25/v1/ (its own pointer, failure records and retention; the GFS prefix is never touched).
Fields, with the GFS names the browser reads:
  hs    swh   significant height of combined wind waves and swell
  tp    pp1d  peak wave period (1/fp of the 1-D spectrum): a true Tp, comparable with GFS-Wave's PERPW
  pdir  mwd   MEAN wave direction, degrees true, FROM (the free data has no peak direction; owner 2026-10-14:
              the particles follow it)
  hs, tp and pdir are left empty where swh < 0.1 m: ecWAM keeps the sea-ice pack as near-zero waves with a noisy
  period, which GFS-Wave leaves empty (mask_calm, G30a F1)
  wind, wdir  from 10u / 10v, as for GFS
The grid is the site's (1440 x 721, 90 N .. 90 S, columns from 180 E = -180 E): no roll. A 0.1-deg grid (ECMWF has
said the open data moves to it) is regridded to the site grid in decode.to_site_grid_ecmwf, so the frames keep their
shape (owner 2026-10-14).
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import decode as D            # noqa: E402
import ecmwf_fetch as EF      # noqa: E402
import publish as P           # noqa: E402
import run as R               # noqa: E402

LICENCE = "https://creativecommons.org/licenses/by/4.0/"
# ECMWF's open-data terms (CC BY 4.0 plus ECMWF's own): a copyright statement, the source, the licence statement with a
# link, the disclaimer; CC BY asks for an indication of changes; nothing may suggest ECMWF endorses the product.
ATTRIBUTION = ("© {year} European Centre for Medium-Range Weather Forecasts (ECMWF). Source: www.ecmwf.int. "
               f"Licence: CC BY 4.0 ({LICENCE}). Based on data and products of ECMWF, modified by Allshore Surf: "
               "quantised, calm water and sea ice left empty, coast-filled and rendered. ECMWF does not accept any "
               "liability whatsoever for any error or omission in the data, their availability, or for any loss or "
               "damage arising from their use, and does not endorse this product.")
CALM_M = R.PDIR_WAVES_M                 # 0.1 m: below it the wave fields are left empty (see mask_calm)
MODEL = {
    "name": "ECMWF IFS wave (ecWAM) + IFS", "grid": "global 0.25 deg (1440x721)",
    "attribution": ATTRIBUTION.format(year="2026"),         # the published copy carries the run's year (EcmwfSource.model_for)
    "licence": LICENCE,
    "fields": {
        "hs":   {"label": "Wave height", "grib": "swh", "definition": "Significant height of combined wind waves and swell"},
        "tp":   {"label": "Peak period", "grib": "pp1d", "definition": "Peak wave period Tp = 1/fp of the 1-D wave spectrum (ECMWF ecWAM)"},
        "wind": {"label": "Wind speed", "grib": "10u/10v", "definition": "IFS 10 m wind speed (sqrt(u^2+v^2))"},
        "pdir": {"label": "Wave direction", "grib": "mwd",
                 "definition": "Mean wave direction from ECMWF ecWAM: degrees true, the direction the waves come FROM (a mean over the spectrum, not the peak)"},
        "wdir": {"label": "Wind direction", "grib": "10u/10v",
                 "definition": "IFS 10 m wind direction: degrees true, the direction the wind blows FROM (atan2(-u, -v))"},
        "pres": {"label": "Sea-level pressure", "grib": "msl",
                 "definition": "IFS mean sea level pressure, hPa (the isobars drawn over the wind overlay)"},
    },
}
FIELD_PARAMS = {"hs": "swh", "tp": "pp1d", "pdir": "mwd"}


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)


def mask_calm(grids):
    """G30a F1: ecWAM keeps sea-ice cells as wave points with a near-zero height (median ~0.04 m) and a meaningless
    peak period (up to the 29-s ceiling), where GFS-Wave leaves them empty. The wave fields (hs, tp, pdir) are left
    empty wherever swh is below CALM_M (0.1 m, the job's calm line for directions) or missing: on run 2026101412 that
    empties ~78-82k cells a step, 99 % poleward of 55 deg, where GFS's frame is empty on 89 % of them, and keeps the
    waves ecWAM carries into the marginal ice zone (GFS draws 72 % of the under-ice cells with >= 0.5 m). Directions
    wrap to [0, 360) (mwd can read 360.0002). Wind is untouched. In place; returns the number of cells emptied."""
    hs = grids["hs"]
    calm = ~(hs >= CALM_M)                           # NaN compares False: missing counts as calm
    emptied = int(np.count_nonzero(calm & ~np.isnan(hs)))
    for name in ("hs", "tp", "pdir"):
        grids[name] = np.where(calm, np.float32(np.nan), grids[name]).astype(np.float32)
    grids["pdir"] = np.mod(grids["pdir"], np.float32(360.0)).astype(np.float32)
    return emptied


class EcmwfSource:
    name = "ECMWF"
    tag = "ECMWF "
    prefix = P.ECMWF_PREFIX
    prune_legacy = False
    stale_fail_h = 30                   # no complete run while the live one is older than this: fail loudly (G30a F2)

    @property
    def model(self):
        """The manifest block, its copyright year the current UTC year (a run is published within hours of its cycle)."""
        return dict(MODEL, attribution=ATTRIBUTION.format(year=_now().year))
    steps = EF.STEPS
    schedule = EF.STEP_SCHEDULE

    failed_retry_after_s = 3600         # a build that failed is retried after 1 h (GFS: 3 h), up to 6 times (GFS: 3):
    failed_max_attempts = 6             # a cycle comes only every 12 h

    def latest_complete_run(self):
        """A throttled listing means "try again next tick", like no run yet (exit 3 or the stale alarm)."""
        try:
            return EF.latest_complete_run()
        except EF.Throttled as exc:
            print(f"ECMWF listing throttled ({exc}); trying again next tick")
            return None

    def fetch(self, run_dt, step):
        """Both streams at once (each reads its records at once, ecmwf_fetch.READ_WORKERS). A build still fetching
        BUILD_BUDGET_S after main() started is given up as not ready (exit 3, the next tick starts again): the job's
        60-min limit would kill it as a failure (and mail the owner)."""
        if _started[0] is not None and time.monotonic() - _started[0] > BUILD_BUDGET_S:
            raise EF.NotReady(f"build still running after {BUILD_BUDGET_S // 60} min (step {step}): giving up for this tick")
        with ThreadPoolExecutor(2) as pool:
            oper = pool.submit(EF.fetch_records, run_dt, step, "oper", EF.OPER_PARAMS)
            wave = EF.fetch_records(run_dt, step, "wave", EF.WAVE_PARAMS)
            return wave, oper.result()

    def decode(self, records, run_dt, step):
        """{hs, tp, pdir, wind, wdir, pres} on the site grid (the GFS job's names and order)."""
        wave, oper = records
        grids = {name: D.decode_ecmwf(wave[p], p, run_dt, step)[0] for name, p in FIELD_PARAMS.items()}
        mask_calm(grids)
        u, _ = D.decode_ecmwf(oper["10u"], "10u", run_dt, step)
        v, _ = D.decode_ecmwf(oper["10v"], "10v", run_dt, step)
        grids["wind"] = D.wind_speed(u, v)
        grids["wdir"] = D.wind_dir_from(u, v)
        grids["pres"] = D.hpa(D.decode_ecmwf(oper["msl"], "msl", run_dt, step)[0])
        return grids


SOURCE = EcmwfSource()
# The build budget (2026-10-15 12Z: a throttled build ran into the job's 60-min limit): 45 min from main(), the rest of
# the hour for the manifest, the pointer and the runner's own setup.
BUILD_BUDGET_S = 45 * 60
_started = [None]


def main(argv=None):
    _started[0] = time.monotonic()
    try:
        return R.main(argv, src=SOURCE)
    finally:
        _started[0] = None


if __name__ == "__main__":
    sys.exit(main())
