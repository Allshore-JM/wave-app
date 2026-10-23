Crops of the published places files (format places-v1 / lines-v1, data v2) (tools/places; Natural Earth, public domain; GeoNames, CC BY 4.0, see
tools/places/LICENSE-PLACES.txt), built 2026-10-23 (plan section 51; the places crop again from v2 in the G34 fix round), used by tests/ui/places.test.js:

- `40_-120.l.bin`, `30_70.l.bin`: the published tier-1 line cells `static/places/v2/c/<name>.l.bin` (byte-identical in v1) (the US west coast
  states; the Kashmir lines of kind 2) with `lines_expect.json` = the Python decoder's reading of them.
- `world-places-crop.bin`: the places of `static/places/v2/world-places.bin` (re-cropped from v2 after G34 F-A7: the
  same 494 records as the first v1 crop, with the corrected v2 names) inside the Pacific box 10 S..60 N,
  180 W..100 W plus Japan (494 places, the same format and q), and `20_-160.p.bin`: the published tier-1 places cell of
  Oahu (132 GeoNames towns); `places_expect.json` = the Python decoder's reading of both (q, cell, count, and per place
  [lat x q, lon x q, rank, mz10, pop, flags, name]).
