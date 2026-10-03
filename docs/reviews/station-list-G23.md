# G23: the forecast-station list without the model boundary points (plan section 32)

Change: `feat/prune-stations` @ 591e23c. `station_list.json` 4,036 -> 734 ids; `station_coords.json` and
`station_timezones.json` whole. One fresh-context reviewer at HIGH, 2026-10-03, read-only.

**Result: 0 P0, 0 P1, 0 P2, 8 P3.**

## Confirmed
- NOAA's own point list (`parm/wave/wave_gfs.buoys.full`, NOAA-EMC/global-workflow, develop) labels every removed id
  as boundary data: 3,118 `IBP` + 184 `BPT` (author re-counted the same numbers). Kept: 480 `DAT`, 4 `XDT`, 244 `VBY`,
  5 buoys not in that file, 1 `BPT` (DIABLO_01). NOAA's section comments name every removed family ("Hawaii BPT" for
  HNL51-68, "NWPS Beta Testing sites (NW)", "HWRF wave grid boundary points", "NHC domain boundary points").
- Geometry without the rule: 3,137 removed points on evenly spaced straight runs of 4+, 156 more on straight lines of
  their family, 9 corners or end pieces; none standalone or near a surf spot.
- Every SWAN station, DEFAULT_STATIONS, the 45 ids of `check_points.py` and every station id in the tests, fixtures,
  template and `static_ui` remain listed.
- Time zones: the coordinate and zone files are byte-identical to 9747ba2; no time-zone path reads the station list.
  Removed ids still answer in their own zone (HWRFe-50 Pacific/Tarawa, same table as production).
- Old links: `_STATION_RE` accepts hyphenated ids; the page shows them as `data-unknown`; `/api/forecast` works on the
  test site for HWRFe-50, RW-NH1-51, NW-HFO51.
- Test site `/stations.json`: 734 entries, the repo's ids in its order, each identical to its production entry.
- Tests: the three named files 90 passed, full suite 599. Mutants on copies of the data (a re-added boundary id, a
  buoy swapped for a boundary id, a dropped named point, pruned coordinate or zone files, a duplicate) are all caught.

## P3 findings and outcomes
| # | Finding | Outcome |
|---|---|---|
| 1 | Production edge-caches `/stations.json` (max-age 3600): old markers can show up to an hour after the deploy; clicking one loads its forecast with an empty select. | Accepted (plan); purging that URL in Cloudflare right after the deploy is optional. |
| 2 | A point's time zone can come from a hidden boundary point (the rule searches all 4,036). | By design, documented in README; told to the owner. |
| 3 | DIABLO_01 is NOAA `BPT` but kept. | Kept: a single point, no box (owner's wording). README names it. |
| 4 | README: CDIP called another country's model; "461 buoys and 273 named points" hides buoys among the named. | Fixed (README). |
| 5 | README could cite NOAA's TYPE column as the rule's source. | Fixed (README). |
| 6 | The order of the list is not tested. | Accepted: order only affects the dropdown; the change kept NOAA's order. |
| 7 | Some kept virtual buoys line up (V14065-69 along 42 E, V23019-22 at 5 S, CARCOOS01-05 around Puerto Rico, Alaska_NS1-3). | Kept: NOAA virtual buoys, not boundaries; told to the owner. |
| 8 | "3,238 on runs" depends on the tolerance (reviewer: 3,137 / 3,125). | No change (not in the README). |

## Not checked
The map in a browser (author's step 2 did), NOAA's operational vs develop point list, a production deploy behind
Cloudflare.
