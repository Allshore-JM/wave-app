# Nearshore analysis data (plan section 109)

Data for `tools/model_frames/nearshore.py`, kept off the code branches. The publisher's workflow checks this branch out
at a pinned commit.

- `pilot/table.npz`: SWAN transfer table of the pilot stretch (Himalayas to Chun's, Oahu), 10 m grid, surf band only.
- `pilot/atlas.json`: best buoy swell per 30 m cell (not yet calibrated).
- `pilot/tides.json`: NOAA CO-OPS predicted highs and lows at Haleiwa (1612668).
- `pilot/manifest.json`: version and sha256 of the three files.

Built by `tools/model_frames/nearshore_data.py` from the model folder. A modelled estimate, not safety guidance.
