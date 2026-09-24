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
downloads the archive (pinned by sha256), builds, and runs `--check` (format key, per-file sizes and
vertex counts, ring orientation and cell containment, the world land area within 2 % of 22,100 square
degrees, and eleven land/water landmark probes); with `upload` true it also publishes `LICENSE.txt`,
the data files and `index.json` (last) to the frames bucket under `static/coast/v1/` with immutable
caching, then reads the index back. The objects are never touched by the frame job's pruning, which
only deletes 10-digit run folders. The upload refuses a prefix that already holds a different build:
browsers and the edge cache keep every object for a year, so a changed build goes under a new prefix
(`static/coast/v2/`) together with a client release that reads it. The `replace` input (or `--replace`)
overrides that guard only when you have decided to accept a year of mixed caches.

What to expect at the tier hand-over (zoom 6 to 7): tier 0 is GSHHG's own 1 km generalisation, so islands
under roughly 15 km² appear only at zoom 7 and above, and at high latitudes (60° N and beyond) the tier-0
edge can sit about one pixel off the imagery at zoom 6 until the full-resolution cells take over.

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

GSHHG is distributed under the GNU Lesser General Public License, version 3 or later, with the
permission notice reproduced in `LICENSE-GSHHG.txt` beside this file (the archive's `LICENSE.TXT`
followed by the LGPL text; the GPL, which the LGPL incorporates by reference, is at
https://www.gnu.org/licenses/gpl-3.0.txt). The files built here are a modified, reformatted subset of
GSHHG and are redistributed under the same licence: the notice is published with the data as
`static/coast/v1/LICENSE.txt`, `index.json` names the licence, and the map panel credits
"coastlines from GSHHG (Wessel & Smith), LGPL" with a link to that notice. The builder in this public
repository is the corresponding source. Cite: Wessel, P., and W. H. F. Smith (1996), A global,
self-consistent, hierarchical, high-resolution shoreline database, J. Geophys. Res., 101(B4), 8741-8743.
