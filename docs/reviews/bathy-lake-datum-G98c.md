# G98c: the Great Lakes datum line in the bathymetry panel (plan section 98)

The bathymetry panel said "Depth below mean sea level" for every region. The Great Lakes regions (lake-<name>,
lake-<name>-dem, uploaded later) hold depths below each lake's Low Water Datum. The panel's note and the readout's
dry-ground text now follow the sources in view, from each source's `datum` text in its region index
(`bathyLake`: `/low water datum/i`).

## Built

| Commit | What |
|---|---|
| 80db1a3 | the note: mean sea level / the lake's Low Water Datum / both; dry ground "dries at the lake's Low Water Datum" (overlay 2.27.7) |
| a49c222 | tests/bathy/test_bathy_lake_datum.py: the client's rule pinned against the builder's source texts |
| 879bb54 | `BathyLayer.sourcesInView` carries `datum` (found on the test site: the note always said mean sea level; overlay 2.27.8) |
| e109d77 | the review's fixes (overlay 2.27.9) |

## Test site (2.27.8, local lake-michigan tiles served through DevTools Fetch, headless Chrome)

- 8 views: Holland z11-z15 the lake note; z6 Great Lakes + Atlantic both; Hawaii z9 the sea note; back to Holland the
  lake note; one note element, rewritten in place.
- Readout = drawn pixel 1,440 / 1,440 per view; hover 20 / 20 in US and Metric units; dry ground 8 / 8 "the lake's Low
  Water Datum".
- Phones 375x812 and 812x375: the lake note, no sideways scroll; long presses 6 / 6. Problems 0.

## Review (one fresh reviewer on 75ac9bc..879bb54): 0 P0, 0 P1, 0 P2, 3 P3

| # | Finding | Outcome |
|---|---|---|
| P3-1 | `validBathyIndex` dropping `datum` (the test site's bug at another place) passed both suites | Fixed: a published index with a lake datum goes through `validBathyIndex` into the layer's list (pinned; the mutant now fails) |
| P3-2 | the panel's fold left `ui.bathyNote` pointing at a removed element (harmless, wasted work) | Fixed: the fold forgets it; the note is written only on a change |
| P3-3 | the Python pin found lake sources by builder markers only | Fixed: lake regions found by `seed "lake"` or a `lake-` name, every source of them checked (a mutant without markers and datum text now fails) |

Held up: all 21 lake-region sources match the rule and no ocean source does; the published indexes of the 10 live
regions (33 sources) do not match, so their panel text is unchanged; the readout's source is the pixel's own region's
index entry, also for a stand-in tile and a filled pixel in a `-dem` region; the note before the layer exists, while
loading, after a field switch and after a fold. Reviewer's mutants 14: 11 killed by the JS suite, M1 / M2 by the
Python pin, M12 fixed above.
