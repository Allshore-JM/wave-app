# Radar rain / mix / snow (plan section 89): test site and review G58

Branch `feat/radar-panel`. The job half (177dd4b, step 3) and the client half (acc193e + 5356ba9, step 4,
overlay 2.26.1) were reviewed together, before either reaches production. Step 1 (the notes removed and the
transparency slider, overlay 2.25.13) is already live (tag `prod-pre-radarpanel`).

## The owner's picks (2026-11-08)
- Palette A: snow lavender to purple, mix pink (sheet of real typed frames, the owner's choice of two).
- The mix band kept at -0.5 to +1.5 C wet-bulb. Measured for the choice: the narrow band 0 to +1 C would have
  turned CONUS mix 48.7k px into 30.0k and snow 16.4k into 22.9k on the same slot.

## Step 4: test site (test @ d12d487, overlay 2.26.1, served = the commit)
Local typed MRMS frames (slot 202611081950, built by the step-3 job) served into the real pointer through
DevTools Fetch, headless Chrome with its own profile, trusted input:
- Quebec, British Columbia and western Alaska typed; Europe, Japan and Taiwan unchanged (rain colours).
- Hover at 32 points = the type and dBZ an independent read of the frames gives (8 rain, 8 snow, 16 mix).
- The "Snow" and "Mix (est.)" bars only while a typed region with frames is in view; labels at the light end.
- Desktop 1280x800 and phone 375x812: no sideways scroll; playback over 13 frames; 0 exceptions.

## G58: one fresh reviewer (diff 1a6d018..5356ba9)
Real NOAA files (45 requests, all five MRMS domains), code reading, 51 mutants (21 client, 30 job).

Held up:
- PrecipFlag and wet-bulb share the reflectivity's grid in every domain; bit depths and identities as the job
  expects; the band's edges and -999 exact; flag values seen only {-3, 0, 1, 3, 6, 7, 10, 91, 96}.
- The code seams (5 / 75 dBZ, 142|143, 213|214, 249|250), the pooling, the pointer shape.
- Client: readout = drawn colour on 6,042 pixels across rain | snow | mix edges and dry gaps; codes 250-255
  never drawn.
- The live client 2.25.13 accepts the typed pointer but draws snow and mix pixels blank and rain | typed blends
  as rain at 83-126 dBZ: the client must reach production first, the job at least 6 h later (the plan's order).

Findings: **0 P0, 0 P1, 0 P2, 4 P3.**

| # | Finding | Outcome |
|---|---|---|
| P3-1 | Mix only where the flag says rain or snow; the brief's summary said "whatever the flag". An echo pixel with flag -3 (outside PrecipFlag's coverage, 1.8 % of CONUS echo today) stays rain | No change: the code follows the plan ("MIX where the pixel is rain or snow by the flag"); the brief's wording was loose. Pinned by test_flag_types_marks_snow_mix_and_unknown_values |
| P3-2 | A wet-bulb file that hangs cost up to ~2 min per domain per tick (two tries of 60 s); five domains could pass the 5-minute deadline | Fixed @ 634cc8f: the wet-bulb file is asked once a tick (`tries=1`); after one that does not answer, the other regions' wet-bulb files are not asked that tick (`_TW_DOWN`, cleared at each start). A hung bucket costs one file's limit (60 s) a tick. Test with a refusing bucket |
| P3-3 | The CONUS wet-bulb decode peaks near 0.7 GB for about a second (24-bit planes through float64), not "about 25 MB" (that is what is held) | README corrected; no code change (0.9 s, the runner has 7 GB) |
| P3-4 | Test gaps: J18 (a dry node beside a typed one), J13 (bars without a typed frame), J23 (each bar under its own name), M33 (the snow / mix counts in the frame note) | Fixed @ 634cc8f: one client test (the three mutants killed) and one job test on the real fixture |

Equivalent survivors: J05, M05, M23.

## Suites after the fixes (634cc8f)
- Radar job: test_radar, test_radar_types, test_radar_sources, test_radar_asia: 342 passed.
- Overlay Node suites: 616 passed. Flag + page tests: 63 passed.
- The test branch got the client test (d3ca62f; test only, the served assets unchanged).

## What follows (two production stops, each with the owner's go-ahead)
1. The client: tag `prod-pre-radarsnow` at the production head; overlay 2.26.1.
2. At least 6 hours later, the job: tag `prod-pre-radarsnow-job`; the first typed slots read back against
   NOAA's files.
