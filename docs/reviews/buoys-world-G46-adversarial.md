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

## Step 3: G46 (one fresh reviewer, 074ccd7..ed11bb6)

Report: scratch `g46/g46-report.md`. **0 P0, 1 P1, 5 P2, 2 P3 groups.** Held up: the suites; dedup on the real lists (no
new marker within 15 km of another network's, the only hidden ones the three Antilles copies); KMA's real node (32
stations, no duplicates, the peak rule's one drop correct); SiMCosta's filter, epoch times, partitions, FROM directions,
disclaimer-only terms; Copernicus's float32 variables without scale factors, VDIR FROM, a silent host costing no more
than before. Mutants 46 applied, 31 killed (survivors listed in the report).

| Finding | Severity | Outcome |
|---|---|---|
| F1 Meteo-France's relays carry a 0-3 code in VTPK; with a 3-4 s mean the ratio rule kept "2-3 s peaks" (6100001 live) | P1 | Fixed: `_vtpk_is_code` drops a file's VTPK column when every value is a whole number 0-3 and one is 0 or 1 (verified on 6100001 / 6100002 / 6200001: codes; the Bothnian Sea, RON, NIOT, the Canaries: periods) |
| F2 "Europe unchanged" was false: 22 new European platforms; the column rule fills windows empty on production (6200025, Troll-A, Soller); the 2-s floor dropped real Bothnian 1.8-1.9 s peaks | P2 | The first two are intended (recorded in the README); the floor is 1 s now (the relays are caught by the code rule) |
| F3 drifters drawn at the middle of the day's box (up to 60 km across; the fixture drifter 11.9 km off) | P2 | Fixed: the newest fix of the newest file, read when the index shows the file grown (cache per file and end time); the box's middle when unreadable |
| F4 a requests timeout / reset on one SiMCosta platform, the list, or a secondary KMA variable failed the whole feed | P2 | Fixed: those handlers catch `requests.RequestException` and `OSError` too |
| F5 SiMCosta with every platform failing republished the old rows as a good refresh, forever | P2 | Fixed: every platform failing is a failed refresh (keep-last, its expiry, `/healthz`) |
| F6 RJ-4 showed the time-domain Hsig (~7 % below its HM0; every other network shows HM0) | P3 | Fixed: HM0 first, Hsig when HM0 is missing or out of range |
| F7 a fill-value TIME in the day-before file blanked today's window | P3 | Fixed: such a row is skipped |
| F7 WIS2 units not checked (25 cm would show as 25 m) | P3 | Fixed: heights m, periods s, directions deg, temperatures K / Celsius; anything else left out |
| F7 KMA's unordered answer under `limit=5000` | P3 | Recorded: 775-816 features a variable today; the warning stays |
| F7 zone names of a few new markers (Pyongyang, Jakarta, Madrid; same offsets) | P3 | Recorded (the live buoys' nearest-civil rule, unchanged) |
| F7 the streaming golden patches out the additions; no test of a first-column national file | P3 | Fixed: new tests (first-column file, the code rule on a copied relay file, the drifter fix, the bad TIME) |
| F7 QC flags not read | P3 | Recorded (as before for every Copernicus platform) |

Fix @ 5df40bb (+ tests + README). Tests 52 in `tests/test_buoys_world.py`; full pytest 2,118; fix mutants 16 of 16
killed (after three pins: a 4 in the code column, an SST in degF, the drifter cache pruned).

## Test-site check of the G46 fixes (test @ c6c705f, live 01:51 UTC)

- Meteo-France's relays 6100001 / 6100002 / 6200001 / 6101031: no peak period in any of their 18-19 rows; the Bothnian
  Sea shows its 1.8-2.7 s peaks (48 rows); RON 6100217 unchanged.
- All 20 drifters drawn exactly at their file's newest fix (0.00 km; own h5py read).
- Values vs the agencies again: Copernicus 35 of 35 windows (the three Antilles copies now merged), SiMCosta 3 of 3 (HM0),
  KMA 32 of 32.
- Windows by trusted clicks and taps, desktop and phone: 9 of 9 each, no console problems. Feeds clean; Copernicus's
  list refresh 61 s on the free instance (53 s before: the drifters' 20 file reads); RSS 196 MB.

## Production (2026-11-01)

Owner "proceed with the production merge". Tag `prod-pre-buoysworld` @ 074ccd7 pushed; `Live-Buoy-Update` fast-forwarded
074ccd7..7f86719; new instance 02:02:47 UTC, 0 failed polls across the switch. Verified on allshoresurf.com: 770 markers
(CMEMS 244, KMA 32, SiMCosta 3; IOOS 48); the 51201 / 51003 pages differ from the pre-merge capture only by the KMA
link-id lines, logo 0; windows = the agencies (Copernicus 35/35, SiMCosta 3/3, KMA 32/32); Meteo-France relays no peak
period, Bothnian peaks shown, 20 drifters 0.00 km from their newest fix; `/healthz` ok, no feed error (Copernicus list
19.6 s, SiMCosta 5.0 s, KMA 3.3 s), RSS 239 MB at 5 minutes; every workflow green on 7f86719; headless Chrome desktop +
phone 9/9 windows each, no console problems; the production origin's storage emptied. Rollback: re-push
`prod-pre-buoysworld`.
