# Coastline data for the model overlays

The wave-height and peak-period overlays are clipped to the ocean in the browser so the field
stops exactly at the coastline. The land polygons come from **GSHHG 2.3.7** (Global Self-consistent,
Hierarchical, High-resolution Geography Database) and are built into the compact `coast-v1`
format by `build_coast.py`; see its docstring for the file layout.

- Tier 0 `world-i.bin`: GSHHG intermediate resolution (~1 km), one file (about 1.3 MB), used for
  map tiles at zoom 6 and below.
- Tier 1 `f/<lat>_<lon>.bin`: GSHHG full resolution (WVS, roughly 100-200 m), one file per
  5-degree cell that contains land (about 1,470 files, 21.5 MB in total, largest about 400 KB),
  fetched only for the cells in view at zoom 7 and above.
- `index.json` lists the tier-1 cells; a cell that is not listed has no land.

Land is GSHHG level 1 plus level 5 (the Antarctic ice front). Lakes count as land: GFS-Wave has
no wave data on lakes or most inland seas.

## Build and publish

Run the `coast-build` workflow by hand (Actions, coast-build, Run workflow). With `upload` false it
downloads the archive (pinned by sha256), builds, and runs `--check`; with `upload` true it also
publishes to the frames bucket under `static/coast/v1/` with immutable caching, `index.json` last.
The objects are never touched by the frame job's pruning, which only deletes 10-digit run folders.
Any change to the format or the content goes under a new prefix (`static/coast/v2/`) together with
a client release that reads it; never overwrite `v1` objects (browsers and the edge cache keep them
for a year).

Locally:

```
python tools/coast/build_coast.py --zip gshhg-bin-2.3.7.zip --out build/coast/v1
python tools/coast/build_coast.py --check build/coast/v1
python -m pytest -q tests/coast
```

Source archive: https://github.com/GenericMappingTools/gshhg-gmt/releases/download/2.3.7/gshhg-bin-2.3.7.zip
(the GMT project's copy of the official release; sha256
`28600e8f7a08645aab43079326df6504212ec5ccb2b4bcf3b5f4f12ed60e82bc`). The SOEST home page,
https://www.soest.hawaii.edu/pwessel/gshhg/, is the canonical source.

## Licence and citation

GSHHG is distributed under the GNU Lesser General Public License (version 3 or any earlier
version). The files built here are a derived, reformatted subset of GSHHG and are redistributed
under the same licence. Cite: Wessel, P., and W. H. F. Smith (1996), A global, self-consistent,
hierarchical, high-resolution shoreline database, J. Geophys. Res., 101(B4), 8741-8743.
The map panel credits "coastlines from GSHHG (Wessel & Smith)".
