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
  wind, wdir  from 10u / 10v, as for GFS
The grid is the site's (1440 x 721, 90 N .. 90 S, columns from 180 E = -180 E): no roll. A 0.1-deg grid (ECMWF has
said the open data moves to it) is regridded to the site grid in decode.to_site_grid_ecmwf, so the frames keep their
shape (owner 2026-10-14).
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import decode as D            # noqa: E402
import ecmwf_fetch as EF      # noqa: E402
import publish as P           # noqa: E402
import run as R               # noqa: E402

LICENCE = "https://creativecommons.org/licenses/by/4.0/"
MODEL = {
    "name": "ECMWF IFS wave (ecWAM) + IFS", "grid": "global 0.25 deg (1440x721)",
    "attribution": ("Based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF), "
                    f"open data under CC BY 4.0 ({LICENCE}); modified: quantised, coast-filled and rendered by "
                    "Allshore Surf. ECMWF does not endorse this product."),
    "licence": LICENCE,
    "fields": {
        "hs":   {"label": "Wave height", "grib": "swh", "definition": "Significant height of combined wind waves and swell"},
        "tp":   {"label": "Peak period", "grib": "pp1d", "definition": "Peak wave period Tp = 1/fp of the 1-D wave spectrum (ECMWF ecWAM)"},
        "wind": {"label": "Wind speed", "grib": "10u/10v", "definition": "IFS 10 m wind speed (sqrt(u^2+v^2))"},
        "pdir": {"label": "Wave direction", "grib": "mwd",
                 "definition": "Mean wave direction from ECMWF ecWAM: degrees true, the direction the waves come FROM (a mean over the spectrum, not the peak)"},
        "wdir": {"label": "Wind direction", "grib": "10u/10v",
                 "definition": "IFS 10 m wind direction: degrees true, the direction the wind blows FROM (atan2(-u, -v))"},
    },
}
FIELD_PARAMS = {"hs": "swh", "tp": "pp1d", "pdir": "mwd"}


class EcmwfSource:
    name = "ECMWF"
    tag = "ECMWF "
    prefix = P.ECMWF_PREFIX
    model = MODEL
    prune_legacy = False
    steps = EF.STEPS
    schedule = EF.STEP_SCHEDULE

    def latest_complete_run(self):
        return EF.latest_complete_run()

    def fetch(self, run_dt, step):
        return (EF.fetch_records(run_dt, step, "wave", EF.WAVE_PARAMS),
                EF.fetch_records(run_dt, step, "oper", EF.OPER_PARAMS))

    def decode(self, records, run_dt, step):
        """{hs, tp, pdir, wind, wdir} on the site grid (the GFS job's names and order)."""
        wave, oper = records
        grids = {name: D.decode_ecmwf(wave[p], p, run_dt, step)[0] for name, p in FIELD_PARAMS.items()}
        u, _ = D.decode_ecmwf(oper["10u"], "10u", run_dt, step)
        v, _ = D.decode_ecmwf(oper["10v"], "10v", run_dt, step)
        grids["wind"] = D.wind_speed(u, v)
        grids["wdir"] = D.wind_dir_from(u, v)
        return grids


SOURCE = EcmwfSource()


def main(argv=None):
    return R.main(argv, src=SOURCE)


if __name__ == "__main__":
    sys.exit(main())
