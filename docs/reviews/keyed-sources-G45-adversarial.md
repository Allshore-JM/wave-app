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
