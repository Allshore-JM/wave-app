# Borders and place names (static/places/v2)

`build_places.py` builds the data behind the map's location aids (plan section 51): country and state/province
borders from Natural Earth, and place names from Natural Earth plus GeoNames towns. The browser loads the tier-0 files
once and the 10-degree cells around the view from zoom 8. The file formats are described at the top of the script.

`static/places/v2/` (2026-10-23) = the first build `v1` plus a name pass (V7-3, the owner's "fix spelling errors only"):
a Natural Earth place keeps its own spelling (København, Montréal), except where its English name `NAME_EN` is spelled
exactly as GeoNames spells the same place (its `GEONAMESID` entry, within 25 km) and differs by at most 2 letters
(Shenyeng -> Shenyang, Poitier -> Poitiers, Bogota -> Bogotá), plus a hand list (Starsy Oskol -> Stary Oskol). The
real build fixes 234 + 1 names and changes nothing else (the lines are byte-identical to v1). `v1` stays published
(immutable) for pages that still point at it.

## Sources

| Source | Files | Licence |
|---|---|---|
| Natural Earth, commit `ca96624a` of nvkelso/natural-earth-vector (pinned by sha256 in the script and the workflow) | `ne_10m_admin_0_boundary_lines_land`, `ne_10m_admin_1_states_provinces_lines`, `ne_10m_populated_places` (GeoJSON; the full places file, which carries each place's GeoNames id) | public domain |
| GeoNames | `cities1000.zip` (changes daily; its sha256 and the date it was built go into `index.json`) | CC BY 4.0 |

`LICENSE-PLACES.txt` is published beside the data as `LICENSE.txt`.

## What is kept

- **Country borders:** admin-0 land boundary lines. Settled borders are kind 0. Lines Natural Earth marks as disputed,
  indefinite, a line of control or a frontier are kind 2, so the map can draw them apart from settled borders. Overlay
  limits, lease limits and unrecognised lines are left out.
- **State and province lines:** kind 1, every admin-1 line except "Unrecognized" and the "boundary indicator" lines
  that cross water (Chesapeake Bay, Long Island Sound, the Columbia mouth). Natural Earth names a line after the highest
  boundary it also marks: a US state line on a census-region edge is "statistical", an Italian province line on a
  region edge is "region".
- **Line zoom:** each line keeps Natural Earth's `MIN_ZOOM`, so the map shows it from that zoom.
- **Places:** every Natural Earth place, at its own `min_zoom`.
- **GeoNames towns:** codes PPLC, PPLA–PPLA5, PPL, PPLF, PPLG, PPLL and PPLR, with at least 1,000 people. Names that
  join several areas with " / " (census areas) are left out. A town shows from zoom 8 at 20,000 people or more, from
  zoom 9 at 5,000 or more, otherwise from zoom 10.
- **Duplicates:** a GeoNames town is left out when it is a Natural Earth place: the same GeoNames id; the same name or
  ascii name within 15 km (Natural Earth points can sit that far from GeoNames'); or one of the town's alternate names
  within 5 km (København, Saint Petersburg). Names are compared folded: case, accents, ø/æ/ß and the like, apostrophes
  and spaces ignored. Of two GeoNames towns with the same folded name within 2 km, the smaller is left out.
- **Ranks:** 0 is a national capital and 1 a state capital. Ranks 2 to 5 go by population: at least 1 million, 100,000,
  10,000, then the rest.
- **Cell cap:** at most 8,000 places per cell; the largest are kept.

## Tiers

| Tier | Contents | Positions |
|---|---|---|
| 0, tile zoom 7 and below | `world-lines.bin`: lines shown below zoom 7.5, simplified to 0.004°; `world-places.bin`: Natural Earth places with `min_zoom` below 7.5 | 1/1000 degree |
| 1, tile zoom 8 and above | `c/<lat>_<lon>.l.bin`: every kept line, unsimplified; `c/<lat>_<lon>.p.bin`: the other places in the cell | 1/10000 degree |

Lines are cut at the antimeridian and at every cell line with the exact crossing point in both pieces, and lines of the
same zoom that share an end point are joined. A few hundred source fragments are shorter than one storage step (about
11 m in tier 1, 110 m in tier 0) and cannot be stored; they are dropped, which is invisible at any map zoom.

## Running it

```bash
mkdir -p src/ne
python tools/places/build_places.py --ne src/ne --geonames src/cities1000.zip --out build/places/v2
python tools/places/build_places.py --check build/places/v2
```

The workflow `coast-build` with input `set: places` downloads the sources, builds, checks and (with `upload: true`)
publishes under `static/places/v2/`.

`--check` validates the format key, every file's header (q, cell size), the sizes and hashes, every piece inside its
cell, the names and the rank order, and logs whether the real-build gates ran. The gates run only for a build from the
pinned Natural Earth files:
- **Pins:** the exact sha256 of tier 0 and of the tier-1 lines. Both depend only on the pinned Natural Earth files and
  the code, so a change of their bytes must be deliberate (update the pin).
- **Recounts from the decoded files:** the Natural Earth places in tier 0 and the cells equal the source count; the
  GeoNames towns decoded equal those kept minus those over the cell cap (at least 120,000); 270 to 310 cells.
- **Line probes, in both tiers:** borders near 49 N 100 W, San Diego and the Rhine at Schaffhausen; state lines at
  42 N 120 W and the Four Corners; the Kashmir line of control as kind 2; no state line across Chesapeake Bay.
- **Place probes:** Honolulu, Tokyo and Paris in tier 0, Hale‘iwa and Kailua in cell `20_-160`.
- **Names:** 220 to 250 names fixed by the agreement rule, every hand fix applied; Shenyang, Nizhnevartovsk, Poitiers,
  Stary Oskol, København and Montréal present, Shenyeng and Starsy Oskol gone.
- **Twin probes:** Tokyo, New York, St. Petersburg, København, Jakarta and Buenos Aires each exist, with no GeoNames
  twin within 20 km.

`--upload` refuses a build without the real-build gates, and a prefix that holds a different build. It prints the
published and local tier-0 and tier-1 hashes when it refuses. Browsers and the edge keep every object for a year, so
each prefix (`static/places/v1/`, `v2/`) is published once. A refresh (a newer GeoNames file, a rule change) goes under a new prefix
together with a client release that points at it.

The real build of 2026-10-23 (GeoNames file of 2026-10-23) had these sizes:
- **Tier 0:** 342 KB of lines (2,721 lines, 144,331 vertices) and 144 KB of places (6,924).
- **Tier 1:** 285 cells, 4.3 MB; the largest cells are 40_0 (45 KB of lines, 185 KB of places) and 40_10 (60 KB,
  182 KB).
- **GeoNames:** 138,786 towns read, 24 " / " names left out, 6,406 Natural Earth places by id, 392 by name, 39 twins,
  131,949 kept, 5,120 left out by the cell cap.

## Country names (`static_ui/countries.json`, plan section 114)

`build_countries.py` builds the country names the map shows at wide zooms from Natural Earth's
`ne_10m_admin_0_countries.geojson` (the same pinned commit; the file's sha256 is pinned in the script). The output is a
small UI asset (about 9 KB) committed to the repository and served with the page's other `static_ui` files, not a
bucket prefix: `static/places/v2` is unchanged.

- **Name:** Natural Earth's `NAME`, or `NAME_EN` where `NAME` is abbreviated (Northern Cyprus, Eswatini, Saint Martin
  and Saint Barthélemy by hand).
  Long names take a common short form (owner, 2026-11-27): USA, DR Congo, UAE, Central African Rep., Bosnia & Herz.,
  "and" pairs as "&", island groups as "Is.". A label still longer than 16 characters is cut onto two lines, never
  right after a one-letter abbreviation ("S. Georgia &" / "S. Sandwich Is.").
- **Shown from:** counted from the screen's widest view (the map's minimum zoom, one world across, rounded down to the
  0.1 step: 2.8 on a 1908-px laptop, 2.4 on a 1440-px screen, about 1.6-1.9 on a phone), by land area on the sphere: 1.5 million km² and more
  at the widest view, 500,000 one zoom closer, 100,000 1.8 closer, 20,000 2.5 closer, the rest 3.2 closer. 32 open-ocean
  island nations (Fiji, Samoa, Tonga, French Polynesia ...) 1.5 closer. The file stores these offsets (x 10); the client
  never starts a name after zoom 5.5 (G80, owner 2026-11-28), so a wide screen still sees the smallest.
- **Gone from:** the zoom at which the country is about 900 px across (the square root of its area at its label
  latitude), never after zoom 7. The client keeps a name at least 1.5 zooms after it came (still never after zoom 7).
- **Left out:** leases and bases, the Cyprus buffer zone, uninhabited banks, reefs and ice, Antarctica, the U.S. Minor
  Outlying Islands, the Australian Indian Ocean Territories, Clipperton, and Spratly Is. (Natural Earth's `NAME_EN`
  calls it "Wake Island").

```bash
curl -sSfLo ne_10m_admin_0_countries.geojson \
  https://raw.githubusercontent.com/nvkelso/natural-earth-vector/ca96624a56bd078437bca8184e78163e5039ad19/geojson/ne_10m_admin_0_countries.geojson
python tools/places/build_countries.py --ne ne_10m_admin_0_countries.geojson --out static_ui/countries.json
python tools/places/build_countries.py --ne ne_10m_admin_0_countries.geojson --check static_ui/countries.json
```

`--check` with `--ne` also requires the committed file to equal a rebuild byte for byte. `tests/places/test_build_countries.py`
pins the committed file's sha256, so a rebuild with new rules must update that pin.
