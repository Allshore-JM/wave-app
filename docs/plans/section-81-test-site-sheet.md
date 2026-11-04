# Section 81, step 4: the satellite layer on the test site (owner's sheet)

Branch `test` @ 81f13b6 carries the client (overlay asset 2.23.2). The jobs are live on production since
8cd6eba (2026-11-04 22:55Z) and both products are on the frames bucket since 23:07Z: `alt/v1/latest.json`
(10 windows, 9 missions) and `sarswell/v1/latest.json` (9 windows, Sentinel-1C + CFOSAT) on models.allshoresurf.com,
dispatched every 10 min by the trigger Worker (the crons :07/:37 and :09/:39 are the fallback).

## What to set (test service first, each with its own go-ahead)

1. `ALT_OVERLAY=1` on the Render TEST service, then redeploy / restart. The page gains "Satellite waves" in the
   Map overlay dropdown and the "Satellite readings" box under Wave height.
2. `SARSWELL=1` (needs `ALT_OVERLAY=1` too): the Heights | Swell switch appears on the layer's own entry.
3. Nothing on production until step 6 (the client merge, tag `prod-pre-alt`).

Without the flags the page is byte-identical to the pre-feature page (`tests/test_overlay_flag.py`).

## What the data looks like today (so a thin picture is not a bug)

- Heights: 9 missions, ~45,000 readings per 3-hour window, the newest about 1-2 h behind the clock. Jason-3 OGDR
  adds only the hours CMEMS has not delivered (tens of readings a window).
- Swell: ONE batch a day lands about 23Z covering up to ~18Z that day. The "last 24 h" of swell are the 24 h before
  the newest point, which is 5-31 h behind the clock; ~150-600 points per window, Sentinel-1C and CFOSAT only.
  The panel's warning appears once the newest point is older than 48 h (12 h for heights).

## Desktop checks

- Map overlay -> "Satellite waves": dots in the wave-height colours, newest on top, faded towards the far end of the
  window; hollow dots near land. Panel: the head "Satellite waves (CMEMS, last 24 h)", the Heights | Swell switch
  (only with `SARSWELL=1`), the wave-height scale, "Readings: last 24 h, newest <time> (<n> h ago) · <k> satellites"
  ("loading i of n…" while chunks land), the credit line, no player, no model choice.
- Hover a dot: "6.2 ft · Sentinel-6A · 11:40 AM HST · model 5.4 ft (+0.8)" ("model —" where the job had no frame).
- Swell: arrows pointing where the swell GOES, colour by the dominant partition's height, length by its peak
  period; hover: "Swell 1: 6.6 ft · 14 s · from 216° | Swell 2: … (Sentinel-1C, 6 h ago)".
- The map credit reads "Overlay: Copernicus Marine" (the title carries both DOIs: moi-00179, moi-00178).
- Switch to Wave height (GFS-Wave and ECMWF): the layer leaves; the model's run, time and play state are as before.
  Tick "Satellite readings": dots within 3 h of the frame in the model's colours; the note beside the box says the
  state ("within 3 h of the frame" / "loading readings…" / "no readings within 3 h of this time"); play: the dots
  follow the frame; hover: "obs 6.2 ft · Sentinel-6A · 11:40 AM HST · model (frame 12:00 PM) 5.4 ft (+0.8)"; the map
  credit gains " · Copernicus Marine". Peak period, wind and Off detach the dots; back to Wave height brings them
  back without a new download. No swell arrows over the model.
- Reload: the box's state and the Heights | Swell choice are kept per tab; a saved "sat" entry with the flag off
  starts Off.
- Leave the tab open 15 min on the satellite entry: the pointer is re-read every 12 min; no flash when a new
  pointer is adopted.

## Phone checks (Safari and Chrome)

- The own entry: the panel folds to "Satellite · newest <time>"; the dots stay tappable (the hover text on tap).
- The box under Wave height fits the phone panel; the note wraps.
- No horizontal scroll from the Heights | Swell switch.

## When something is wrong

- "readings unavailable" in the note / the panel: the pointer did not load (check
  `https://models.allshoresurf.com/alt/v1/latest.json`), or the browser lacks `DecompressionStream` (the box is
  disabled with "not supported in this browser").
- Write down: the browser, the entry, the time shown, what the note said; the sheet's reviewer B pass goes into
  `docs/reviews/`.
