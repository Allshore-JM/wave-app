# Nearshore analysis data (plan section 109)

Data for `tools/model_frames/nearshore.py`, kept off the code branches. The publisher's workflow
(`model-gfsspec.yml`) checks this branch out at a pinned commit.

- `ns/tides.json`: NOAA CO-OPS predicted highs and lows at Haleiwa (1612668), three years.
- `ns/manifest.json`: version, the table set (`ns-1`: Kaena Point to Kahuku Point, six regions) with the sha256 of
  the set's own manifest in the bucket (`static/nearshore/ns-1/manifest.json`), and the sha256 of `tides.json`.

The SWAN transfer tables are not here: the `nearshore-table` workflow builds them into the bucket, and the job reads
them from there and checks each against the pinned manifest.

Built by `tools/model_frames/nearshore_data.py`. The pilot's folder (`pilot/`, one table of Himalayas to Chun's) is in
this branch's history up to commit 8d16aec. A modelled estimate, not safety guidance.
