# Borders and place names (static/places/v1)

`build_places.py` builds the data behind the map's location aids (plan section 51): country and state/province
borders from Natural Earth, and place names from Natural Earth plus GeoNames towns. The browser loads the tier-0 files
once and the 10-degree cells around the view from zoom 8. The file formats are described at the top of the script.

## Sources

| Source | Files | Licence |
|---|---|---|
| Natural Earth, commit `ca96624a` of nvkelso/natural-earth-vector (pinned by sha256 in the script and the workflow) | `ne_10m_admin_0_boundary_lines_land`, `ne_10m_admin_1_states_provinces_lines`, `ne_10m_populated_places_simple` (GeoJSON) | public domain |
| GeoNames | `cities1000.zip` (changes daily; its sha256 and date go into `index.json`) | CC BY 4.0 |

`LICENSE-PLACES.txt` is published beside the data as `LICENSE.txt`.

## What is kept

- **Country borders:** admin-0 land boundary lines, except overlay limits, lease limits and unrecognised lines.
- **State and province lines:** every admin-1 line except "Unrecognized". Natural Earth names a line after the highest
  boundary it also marks: a US state line on a census-region edge is "statistical", an Italian province line on a
  region edge is "region".
- **Line zoom:** each line keeps Natural Earth's `MIN_ZOOM`, so the map shows it from that zoom.
- **Places:** every Natural Earth place, at its own `min_zoom`.
- **GeoNames towns:** codes PPLC, PPLA–PPLA5, PPL, PPLF, PPLG, PPLL and PPLR, with at least 1,000 people. A town shows
  from zoom 8 at 20,000 people or more, from zoom 9 at 5,000 or more, otherwise from zoom 10.
- **Duplicates:** a GeoNames town is left out when a Natural Earth place of the same name (accents and apostrophes
  ignored) lies within 5 km.
- **Ranks:** 0 is a national capital and 1 a state capital. Ranks 2 to 5 go by population: at least 1 million, 100,000,
  10,000, then the rest.
- **Cell cap:** at most 8,000 places per cell; the largest are kept.

## Tiers

| Tier | Contents | Positions |
|---|---|---|
| 0, tile zoom 7 and below | `world-lines.bin`: lines shown below zoom 7.5, simplified to 0.004°; `world-places.bin`: Natural Earth places with `min_zoom` below 7.5 | 1/1000 degree |
| 1, tile zoom 8 and above | `c/<lat>_<lon>.l.bin`: every kept line, unsimplified; `c/<lat>_<lon>.p.bin`: the other places in the cell | 1/10000 degree |

Lines are cut at the antimeridian and at every cell line with the exact crossing point in both pieces, and lines of the
same zoom that share an end point are joined.

## Running it

```bash
mkdir -p src/ne
python tools/places/build_places.py --ne src/ne --geonames src/cities1000.zip --out build/places/v1
python tools/places/build_places.py --check build/places/v1
```

The workflow `coast-build` with input `set: places` downloads the sources, builds, checks and (with `upload: true`)
publishes under `static/places/v1/`. The upload refuses a prefix that holds a different build. A later rebuild has a
different GeoNames file, so it goes under a new prefix, or `--replace` says so.

`--check` validates the format key, the sizes and hashes, every piece inside its cell, the names and the rank order.
For a build from the pinned Natural Earth files it also checks the counts and landmark probes: borders near 49 N 100 W
and San Diego, state lines at 42 N 120 W and the Four Corners, and Honolulu, Tokyo, Paris, Hale‘iwa and Kailua.

The first real build (2026-10-23, GeoNames of 2026-10-22) had these sizes:
- **Tier 0:** 342 KB of lines and 144 KB of places (6,924).
- **Tier 1:** 288 cells, 4.3 MB; the largest cell is 59 KB of lines and 185 KB of places.
- **GeoNames:** 132,938 towns kept, 5,872 duplicates left out and 5,144 left out by the cell cap.
