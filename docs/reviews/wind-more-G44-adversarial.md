# G44: plan section 67, live wind stations from nine more agencies — adversarial review and fix round

Branch `feat/wind-more` (off production 541a897). Reviewed at 8b1904e and re-checked at c2f30b6 by one fresh
reviewer (code, data against the agencies' own answers, the test site). Fixed at a307d1f (+ golden 2c2cde8).

## Findings

**0 P0, 2 P1, 3 P2, 9 P3.**

| # | Finding | Outcome |
|---|---|---|
| P1-1 | JMA windows: every 10-minute row's "gust" is the day's highest gust so far (the point file repeats it on every row) | Fixed during the review (c2f30b6): no gust from JMA, map or point file |
| P1-2 | IPMA direction class 9 (its own label "N") read as "no direction": 129 of 1,865 windy readings at the 84 stations | Fixed: `IPMA_DIR[9] = 0` |
| P2-1 | A window whose agency answers 200 with an unexpected JSON shape (or deeply nested JSON) raised past the window service: HTTP 500 (JMA, IEM, SMHI, DMI) | Fixed: `wind_more.history` turns shape errors and RecursionError into `WindError` (a failed window, asked again in a minute) |
| P2-2 | IEM's obhistory `date=` is the station's local date; the window asked UTC dates, so up to 8 hours went missing every US evening | Fixed: `local_days(since, now, tz)` asks the station's local dates |
| P2-3 | 37 Canadian airfields with two ECCC flags 0.1-1.4 km apart (lake-shore airfields have no METAR site in the snapshot) | Fixed: the new rows that are an airport's own station (ECCC CYxx ids, then stations named like an airport) are airfield sites, placed first; 37 stations left out |
| P3-1 | Two FMI stations in another offset's zone (Europe/Moscow, Europe/Paris) | Fixed: one fixed zone for the agencies on one country's clock (JMA, FMI, SMHI, DWD, Met Eireann); SMHI and FMI also leave timezonefinder's canonical names (Europe/Paris, Europe/Athens) |
| P3-2 | IEM airport ids P53 / P58 / P59 are NDBC's KP53 / KP58 / KP59 relays | Fixed: IEM ids also checked against NDBC's (K / P / T prefixes); 3 left out |
| P3-3 | One reading more than 10 minutes ahead hid the station's whole answer, its 24-hour series too | Fixed: the rows ahead are dropped, not the station |
| P3-4 | A refusal (403 / 429) threw away what answered before it in that refresh; the refused unit was not asked first after the pause | Fixed: what answered is kept; the refused and unasked units go first |
| P3-5 | A one-answer feed answering `{}` / `null` / `{"error": ...}` counted as a good refresh (kept readings aged out silently, `/healthz` green) | Fixed: an answer with no reading of any station is a failed refresh (keep-last; `/healthz` shows it) |
| P3-6 | The window budget counts windows, not requests (a JMA window is up to 9 requests) | README corrected (30 windows a minute per agency, with the requests per window) |
| P3-7 | ECCC / Met Eireann windows right after a restart said "No wind readings in the last 24 hours", cached 10 minutes | Fixed: "No readings since the server restarted; earlier hours are not kept after a restart.", asked again in a minute |
| P3-8 | No time bound on a paced refresh (ECCC up to 240 s if the agency trickles) | Fixed: a paced refresh and ECCC's ask nothing new after 90 s (those go first next time) |
| P3-9 | Licence wording: CC BY sources did not say what was changed nor link the licence; DMI named no licence (it is CC BY 4.0: DMI's terms); ECCC's line read "Source: Data Source: ..." | Fixed: "units converted by allshoresurf.com" on the CC BY sources (DMI included), the window links the licence text (`licence_url`: CC BY sources, JMA, ECCC; https only), no double "Source:" (JMA had it too) |

The reviewer's 43 mutants: 24 killed; every survivor that could hide a defect now has a pin in
`tests/test_wind_more_g44.py` (JST compact stamps, SMHI milliseconds, DMI gust / direction, DWD newest rows, Met Eireann
summer time, the newer reading kept, the refusal stop and cursor, ECCC's "every file failed", the window cut, IEM's
local days and network filter, MORE_KEPT, the agency budget, IEM's units, the SWOB quality boundary, JMA's quality 1).
The IPMA `<= -90` survivor is equivalent (the range checks drop -99).

## The fix round (a307d1f, golden 2c2cde8; UI 1.27.1)

- Snapshot re-placed with `--respot` (no network): 4,510 -> 4,470 stations (37 ECCC airfield duplicates, 3 IEM NDBC
  twins); zones changed for 103 SMHI, 101 FMI and 28 DWD rows (same offsets except the two FMI rows); nothing else moved.
  The reviewer's 37 airfield pairs: all resolved, each field keeps its NAV CANADA station.
- Tests: `tests/test_wind_more_g44.py` (93) and two snapshot-rule tests in `tests/test_more_stations.py`; the window's
  licence link in `tests/ui/winds.test.js`. Fix-round mutants: 20 of 20 killed.

## Owner awareness

- IPMA asks for an e-mail notification to webmaster@ipma.pt (the owner's to send).
- Zone labels such as "GMT+9" are the site-wide rule. The fixed zones fix the canonical names for five agencies;
  ECCC, IEM, DMI and IPMA keep the nearest civil zone, with the right offsets.
- A full rebuild keeps only the stations reporting wind at that moment, so a few come and go between builds;
  `--respot` re-places without network.
- DWD is restricted to Germany's own stations: an author decision following the owner's "DWD Germany".
