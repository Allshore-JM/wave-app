# Section 72: live buoys for the data-poor regions — test site and review (G46)

Branch `feat/buoys-world` (off production 074ccd7). Test site `test` @ c37400b.

## Step 2: test-site verification (2026-11-01 UTC)

Deploy: `test` @ c249a21 (cherry-picks of f8204a2, 2c7b765, 8c52cd7; README the test branch's own) live 00:58 UTC, then
the fix below @ c37400b live 01:07 UTC. Site files = the feature branch except the test site's three logo lines.

- **Counts** (the merged live list): 769 markers vs production's 678. Copernicus 190 -> 245 (335 platforms published
  vs 277), KMA 32, SiMCosta 3 (RJ-4 Rio, RS-4 Rio Grande, CDRJ-1 Niteroi). AODN's Ceduna and Bengello take over from
  AusWaves's copies on the test site because the fresh process found AODN's November spectra files (not a section-72
  change; production does the same at its next AODN probe).
- **Values vs the agencies** (own readers: h5py for the Copernicus files, plain JSON for SiMCosta and KMA; scratch
  `s72/v2/data72.py`): Copernicus 38 of 38 windows (18 INCOIS / NIO drifters, 4 NIOT moored buoys, French Guiana and
  the French Antilles, Mayotte, the Canaries, Italy's RON 6100217, Meteo-France's relay 6200001) equal the platform's
  own file at the shown time and at every row of the 24-hour table (height, peak period, direction); a peak period
  of 0 in the file (4100230) and Meteo-France's 2-s relay value are shown as no peak period, as designed. SiMCosta 3 of
  3, KMA 32 of 32 equal the agency's answer at the shown time.
- **Windows by trusted clicks (desktop 1280x800) and taps (phone 375x812)** in headless Chrome (own profile, never
  signed in): NIOT 2300453, the drifter 7810896, RON 6100217, Canaries 1300130 (its forecast-point link), Martinique
  4100098 (height only), SiMCosta RJ-4 and CDRJ-1 (the swell / wind-sea table), KMA 22305 and 2200308: every one opened
  from its own marker with the agency's reading, the local zone, the 24-hour rows; no sideways scroll on the phone; no
  console problems. KMA 22101 and 22184 link to NOAA's forecast points 22101 / 22184 and back ("Live buoy"), desktop and
  phone.
- **Feeds** (`/healthz`): Copernicus, SiMCosta and KMA published without error (7.6 s SiMCosta, 8.3 s KMA). Copernicus's
  first refresh took 53 s on the free test instance against production's 6.5 s; the index parse itself costs the same
  with the old and the new filter (0.47-0.54 s on one 42.6 MB index, locally): the download on the free instance.
  RSS 212 MB at 6 minutes (production 311 MB after 4 hours).

### Finding V2-1 (P2, fixed): NDBC buoys drawn twice in the Antilles

The Antilles box brought Copernicus's copies of NDBC's 41040, 41044 and 42060 (`GL_TS_MO_41040` ...), 3.0, 6.6 and
12.9 km from NDBC's own markers: two markers for one buoy, with names ("41040" against NDBC's) that the name rule could
not match. Fixed @ f1dd7de: a Copernicus id that is a plain 5-digit number counts as that buoy number in the dedup
(`_CMEMS_NUM_RE`, beside the `_TS_MO_6200NNN` rule), so the copy joins NDBC's marker within the number radius. On the
real list only those three change (Mallorca's 61499 has no twin). Test + 2 of 2 mutants killed. Live on the test site
@ c37400b: the three copies gone, 769 markers.
