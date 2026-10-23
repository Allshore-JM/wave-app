Crops of the published places-v1 files (tools/places; Natural Earth, public domain; GeoNames, CC BY 4.0, see
tools/places/LICENSE-PLACES.txt), built 2026-10-23 (plan section 51), used by tests/ui/places.test.js:

- `40_-120.l.bin`, `30_70.l.bin`: the published tier-1 line cells `static/places/v1/c/<name>.l.bin` (the US west coast
  states; the Kashmir lines of kind 2) with `lines_expect.json` = the Python decoder's reading of them.
- `world-places-crop.bin`: the places of `static/places/v1/world-places.bin` inside the Pacific box 10 S..60 N,
  180 W..100 W plus Japan (494 places, the same format and q), and `20_-160.p.bin`: the published tier-1 places cell of
  Oahu (132 GeoNames towns); `places_expect.json` = the Python decoder's reading of both (q, cell, count, and per place
  [lat x q, lon x q, rank, mz10, pop, flags, name]).
