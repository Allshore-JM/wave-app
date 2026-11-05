# Section 82: the gridded map on the test site (owner's sheet)

Branch `test` @ 077ff90 (overlay asset 2.24.0) with `ALT_GRID=1` (and `ALT_OVERLAY=1`, `SARSWELL=1`) on the Render
TEST service. The data is live since the job-only merge (production 06a965f, 2026-11-05 02:02Z):
`https://models.allshoresurf.com/altgrid/v1/latest.json`.

## What it is
A "Gridded map" box in the Satellite waves panel draws Copernicus Marine's level-4 gridded significant wave height
(every altimeter merged onto a 0.5-degree grid, doi 10.48670/moi-00180) under the dots or the arrows, in the same
wave-height colours. Off by default; the choice is remembered per tab.

## What to expect (so a lag is not taken for a bug)
- The map is the NEWEST step of a product made once a day: one file at ~07:30Z covers 00 / 06 / 12 / 18Z of the day
  before, so the map is 13-37 h behind the clock. The note beside the box says which step and how old
  ("CMEMS L4 · Nov 3, 1:00 PM EST (34 h ago)").
- Blank near the coast (~50 km) and under sea ice: the product does not cover those. No use at a break; context only.
- Past 48 h the panel warns ("The gridded map is 49 h old."); past 72 h the map comes off ("hidden: 73 h old").
- Where a dot and the field agree, the dot blends into the field (same colours): that is the comparison.

## Desktop checks
- Satellite waves -> tick "Gridded map": the field appears under the dots; the note shows the step and its age; the
  credit line adds "doi:10.48670/moi-00180".
- Hover a dot: "... · model 7.1 ft (+0.7) · grid 14.4 ft" (the grid's own value there; it can differ from the
  reading by a day's change). Hover open water: "grid 10.1 ft · Nov 3, 1:00 PM EST".
- Swell: the field stays under the arrows; the readings line reads "Readings: 24 h to <time> (<age>) · n satellites".
- Reload: the box stays ticked. Untick: the field goes at once; tick again: back without a download.
- Wave height, SST, Off: the field goes with the entry.

## Phone checks
- The row "Gridded map" fits the panel, the note wraps under it; no horizontal scroll; a LONG PRESS on open water shows the
  grid value (as for the model fields); a quick tap on a dot shows its reading with the grid's value after it.
  Past 48 h the warning line sits right under the box (also on a 360-px phone).

## Checked by Claude (2026-11-05 03:5xZ, headless Chromium)
All of the above on desktop 1280 x 800 and phone 375 x 812: the box, the draw under the dots and arrows, the note
(34 h), the hovers, the credit, Swell, reload, Off, Wave height and back; the drawn values equal the published frame
at every point probed; no console errors, no failed requests; the phone page 375 px wide.
