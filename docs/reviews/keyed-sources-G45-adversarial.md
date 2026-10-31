# Keyed feeds (plan section 69): test-site verification and review G45

Section 69 turns four keys (registered in section 68) into feeds: UK CCO and Taiwan CWA live buoys, and KNMI
(the Netherlands), MET Norway Frost and Taiwan CWA wind stations. Branch `feat/keyed-sources` off production 1343465.

## Step 2: the test site with the real keys (2026-10-31)

`test` @ 1a0d7b4 (the step-1 code) went live at about 18:43 UTC, with the four keys on the test service. Every fix below
was deployed by cherry-pick and checked again on the site. Checks used `/healthz`, the site's own answers, the agencies'
own pages and files, and a headless Chrome (its own profile, never signed in) with real clicks and taps.

### What worked first time

- **CCO**: 48 sensors listed; CCO's marker won the dedup over CEFAS and Copernicus copies at 28 spots (rank 59 > 58).
  Dawlish on CCO's own page at 18:00 GMT: 0.57 m, Hmax 0.86, Tp 4.8 s, Tz 3.6 s, 124 deg, spread 21, SST 15 C; the
  site: 0.57 / 0.86 / 4.76 / 3.636 / 123.8 / 20.8 / 15.0 at 18:00Z. CCO's page labels its times "GMT".
- **KNMI**: 71 stations found in the files, 69 with a reading about 6 minutes old. At 18:40 UTC the site's speeds,
  directions, temperatures, humidity and pressure equal KNMI's public observations page at every station compared
  (De Bilt, Hoek van Holland, Terschelling, IJmuiden, Lauwersoog, Rotterdam, Schiphol, Twenthe, Vlissingen,
  Voorschoten, Wilhelminadorp). Gusts: 4 of 7 equal; the others are 1-3 km/h lower than KNMI's page (the site reads
  `gff`, KNMI's documented 10-minute gust; the page may show another of its gust fields). Not changed.
- **Frost**: 729 sources inside the coast map, 572 with a reading; values agree with the airports' METAR reports at 54
  shared airfields (units m/s, directions FROM).

### Found and fixed

| # | Finding | Fix |
|---|---|---|
| V2-1 | **CWA**: every request failed: `CERTIFICATE_VERIFY_FAILED: Missing Subject Key Identifier`. Python 3.13 (the site's Python on Render, although `runtime.txt` says 3.11.9) turns OpenSSL's strict X.509 checks on by default; CWA's chain ends at "TWCA Global Root CA", a root in certifi's list without that extension. | Owner decision (2026-10-31): relax only that flag, only for CWA's host. `cwa_tls.py`: the default verifying context on certifi's list without `VERIFY_X509_STRICT`, mounted for `https://opendata.cwa.gov.tw/` on `app.HTTP` and `app._POINT_HTTP`. Checked live: CWA answers through it; a wrong host, an expired, a self-signed and an untrusted-root certificate and both eMudhra test hosts are still refused. `tests/test_swan_cert.py` lets that file alone set a context and pins its flags and mounts. |
| V2-2 | **One flag per spot**: stations KNMI and Frost name at run time skipped the snapshot's rule: 54 Frost stations sat on Norwegian airport flags (0.0 km), KNMI's airports and North Sea platforms on their METAR flags. | `wind_keyed.SpotIndex`: the snapshot tool's rule (100 m of any station; 500 m / 1.5 km of a METAR site; 2.5 km when the name says airport, incl. Lufthamn and "LH", or begins like the site's name; road stations skip the airfield distances, as in the tool). A test pins the numbers to the tool's. Left out: KNMI 24 of 71, Frost 74 of 729; `/healthz` `left_out`. |
| V2-3 | **CCO** lists retired sensors (West Anglesey's last reading is from 2016; Port Isaac 8 days old). | Sensors whose newest wave height is more than 4 days old are left out (CMEMS's rule), and so is a row more than an hour ahead. 45 listed. |
| V2-4 | **CWA buoys**: the station list request answered 404: O-B0076-001 is a file, not a REST dataset (CWA's API document lists only O-B0075-001 / -002). | The station list through CWA's file API with the same key. |
| V2-5 | **CWA buoys**: then "no wave reading": the REST answer stamps times `DateTime` where CWA's file (the fixture's source) says `DataTime`. | Both accepted; an answer without a usable reading now says what it held (field names and counts, never values or the key). 26 buoys. |
| V2-6 | **Live window**: CWA's buoys showed "Peak period 0.0 s" (`Number(null)` is 0, older code: any feed without a peak period). | `isNum`; a missing period is a dash; no Peak period card and no Tp column when only a mean period exists. `tests/ui/buoyperiod.test.js`. |
| V2-7 | **Frost road stations** (Statens vegvesen, "E6 Andelva") were "weather stations". | Kind `road`, as IEM's road networks. |

### Checked after the fixes

- CWA wind: 268 stations, every one equal to CWA's own file at the same time (speed, direction).
- CWA buoys: 12 of 12 windows equal CWA's file (height, mean period, direction, SST); the period is never a peak period.
- No key and no signed address in any public answer (`/healthz`, the live list, both wind lists, histories, a buoy
  window, the page): searched for `Authorization=` values, signed-URL parameters, JWT- and UUID-shaped strings.
- Windows by real clicks (desktop 1280x800): CCO Dawlish, CWA Longdong, KNMI De Bilt, Frost Oslo-Blindern and Utsira,
  CWA Taipei: titles, subtitles ("Wind · KNMI weather station 06260"), readings, 24-hour rows (KNMI filling, CWA since
  the restart), licence lines; the map's credit names the agency of the flags in view. Console clean.
- After the period fix (test @ 51de019), desktop and phone 375x812 (real taps): Longdong shows "SIG. HEIGHT 3.6 ft · MEAN
  PERIOD 5.1 s" with no Peak period card, and its table has no Tp column ("Hs ft · Avg Pd sec · Dir · SST"). Dawlish
  keeps both periods. The phone wind windows were 375 px wide with no sideways scroll. Console clean on both. The test
  origin's storage was emptied afterwards and the browser stopped (no account signed in).
- Suites at 1f85ecd: pytest 2,044 passed, Node 1,066 passed.

### Owner awareness

- KNMI's North Sea platforms and the airports carry METAR flags, so their KNMI stations are left out (the section-67 rule:
  one flag per spot, METAR first). KNMI's 10-minute data would be fresher there.
- CWA's wind file names stations in Chinese only ("臺北"); English names would need CWA's station list dataset (a keyed
  request the snapshot tool cannot make without the key on the laptop).
- CWA wind windows hold only the hours since the server started (as ECCC's), KNMI's fill in about 45 minutes.
- Frost's coast map follows the owner's section-67 rule, so stations near Norway's large lakes (Mjøsa) are kept.

## G45: one fresh reviewer (2026-10-31)

One fresh reviewer read `git diff 1343465..1f85ecd` and the test site @ 51de019 (its report and scripts are in the
session scratch `g45/`). **P0 0, P1 2, P2 9, P3 12 (+ test gaps); 48 mutants, 30 killed.** It confirmed the parts
that matter most: no key or signed address in any answer (40 buoy windows, 7 wind histories, every list); all 268 CWA
wind rows and all 26 CWA buoys equal CWA's own files; CCO's windows equal CCO's page; the TLS exception is exactly one
flag for one host; the found stations follow the snapshot tool's spot rule.

### Findings and what was done

| # | Finding | Outcome |
|---|---|---|
| F-1 (P1) | CCO's buoys drawn twice beside their CEFAS "<site> Waverider" / CMEMS "<site>DWR" or numbered copies at 6-7 UK spots | Fixed: the dedup's name key strips WAVERIDER and a trailing DWR; `_MATCH_NAME_ALIAS` names the numbered copy (6201045 = Scarborough). Found on the test site after this fix: a copy that matches a copy already hidden (CMEMS 6201000 on CEFAS's Penzance Waverider, under CCO's Penzance 1.7 km away) stood alone: it now joins that copy's kept marker within 10 km (7ea4380). |
| F-2 (P2) | KNMI's signed download address not cut from error texts | Fixed: `_SIGNED` cuts any query holding a signature, token or X-Amz part, with or without a scheme. |
| F-3 (P2) | The CWA key could reach urllib3's "Retrying ..." warning | Fixed: a log filter on the urllib3 and requests loggers, installed at the app's start, redacts the key and signed queries (a leak script with the app imported: 0 appearances). |
| F-4 (P2) | The spot index missed METAR flags east or west of a found station north of 63 N | Fixed: 0.1-degree cells, and the east-west search widens with the latitude. |
| F-5 (P2) | A failed daily station-list call stopped every reading refresh (Frost, CWA buoys) | Fixed: the list in hand stays and is asked again in an hour; `/healthz` shows `list_error`. |
| F-6 (P1) | A CCO window could hold a visitor's request thread about 2 minutes | Fixed: a window is one attempt under 20 s, size-capped, one request at a time per sensor; the list stays on the background runner. |
| F-7 (P2) | 23 % of Frost's flags never had a reading | Fixed: a found station is listed once it has brought a reading. |
| F-8 (P2) | A row stamped in the future became the latest reading (CCO, CWA buoys) | Fixed: rows ahead of the clock are left out. |
| F-9 (P2) | CCO's quality flag: mapping unsourced, CCO prints it as four digits | Fixed: a code of several digits with any digit not 0 blanks every wave field; single codes keep the mapping; `/healthz` lists the values seen (live: "9" and none only). |
| F-10 (P2) | KNMI's classic-NetCDF reader could spin forever on a 300-byte file | Fixed: a streaming or impossible record count is refused at once; record bounds checked; parse errors become NcError. |
| F-11 (P2) | A failing KNMI file was re-read every 2 minutes for 24 h | Fixed: given up after 3 tries; "filling" follows the files KNMI lists. |
| F-12 (P3) | KNMI's `left_out` dropped to 0 between files | Fixed: each file starts from every station found so far. |
| F-13 (P3) | `cwa_tls` not built like requests' own context | Fixed: urllib3's context, certifi's list, minus the strict flag; the test compares with requests' preloaded context and checks the proxy path. |
| F-14 (P3) | Stations found after the page loaded were missing until a reload | Fixed: the page asks for the wind list again (at most once a minute) when the readings name a station it lacks. |
| F-15 (P3) | Frost dropped the gust element for the life of the process on any HTTP 400 | Fixed: asked again an hour after a refusal. |
| F-16 (P3) | CWA wind read a time without an offset in the server's zone | Fixed: taken as Taipei's (buoys too). |
| F-17 (P3) | KNMI's reader ignored `scale_factor` / `add_offset` | Fixed: packed values and positions are unpacked; the fill value is read before unpacking. |
| F-18 (P3) | A CCO window showed every other reading | Fixed: 49 half-hours (both ends of the day) kept. |
| F-19 (P3) | Card and table disagreed for a buoy with only a zero-crossing period | Fixed: the card's peak-period rule is the table's (`wantsPeak`). |
| F-20 (P3) | No Frost station north of 72.5 N | Found on the test site: Frost lists Svalbard and Jan Mayen as their own country (SJ). Fixed: a second source call (7ea4380); a failure of that call leaves them out until the next day's list. |
| F-21 (P3) | CCO windows asked by sensor name | Fixed: by id when the answer carries one. |
| F-22 (P3) | CCO / CWA buoy reads checked the size after reading the whole body | Fixed: `_size_capped_get` with headers. |
| F-23 (P3) | KNMI's request budget wording | Fixed in the README (about 40 an hour, about 300 in the 45 minutes after a start; KNMI allows 1,000). |
| Test gaps | 18 surviving mutants | Pinned (the reviewer's list: QC 1, direction kind, the 24-h list TTL, "LH", the cell size, back-fill, gust order, maxage, a found station never replacing a snapshot row, KNMI windows from memory, naive buoy times, Frost batch 404, version stable, TLS minimum / proxy). |

Commits: 137ae25 (the fixes), 43e4abb (golden, own commit), 7ea4380 (the two test-site follow-ups), f527aef (pins),
0c1b2d9 (README).

### Mutants of the fix round

32 mutants on a scratch copy, after pytest: 26 killed at first. Of the six left, five were test gaps and are pinned
(f527aef): the spot rule two cells east north of 77 N, the record count refused by its own check, packed station
positions, the CCO window's 49 rows through `detail()`, and the Svalbard call (the first version of that mutant was a
no-op; two real ones are killed). The last, "recompute the found list when it is unchanged", is equivalent: the same
list gives the same stations and the version moves only with the listed set. 31 of 32 killed.

### Test site after the fixes

`test` @ 41a83db (the fixes), then @ a19b95f (the follow-ups and pins), live 20:34:45 UTC with no failed request.
Checks with `/healthz`, the site's answers and the agencies' pages:

- CCO: 45 sensors; every one of the 45 markers has no other feed's marker within 10 km (673 markers; 678 before the
  follow-up: five standalone CMEMS copies, 6201000 / 6201004 / 6201007 / 6201008 / 6201019 near Penzance, Minehead,
  Weymouth and Chesil, Boscombe and Hornsea, now join CCO's markers). Windows: 49 rows over 24 hours (Penzance, Scarborough). Quality flag values: "9" x 3, none x 67.
- CWA buoys: 26. KNMI: 45 listed, all with a reading, 24 left out on METAR flags. CWA wind: 268, all with a reading.
- Frost: 909 sources (34 north of 72.5 N), 764 inside the coast map, 529 listed (all with a reading), 77 left out;
  29 Svalbard / Jan Mayen stations listed (Bjørnøya, Hopen, Edgeøya, Kongsøya, Sørkappøya, Hornsund ...); gust on.
- No key-shaped text (`Authorization=` values, signed parameters, JWTs) in `/healthz`, the lists or the windows.
- Suites at f527aef + README: pytest 2,066 passed, Node 1,067 passed.

### Owner awareness (unchanged from step 2, plus)

- The Svalbard call is separate: if Frost refuses it, Svalbard's stations are missing until the next day's list
  (`/healthz` names the error); the rest of Norway is unaffected.
