# Section 112: worldwide tides on the Tide layer — review (G76)

Branch `feat/global-tides` (off production a3528d3). One fresh reviewer on a3528d3..3e28a3e.

**2 P0, 1 P1, 9 P2, 11 P3.** Every finding was checked by the author before the fix.

Held up: the predictor (= pyTMD within 2 mm; NOAA's own constants -> NOAA's predictions); LAT applied exactly once, model
cm -> m once; every agency parser's clock against TICON within about +-10 min over a live 34-day window (CHS UTC,
Kartverket tzone=0, JMA JST, HKO HKT, LINZ NZ time, MI UTC, QLD AEST); units; the payload shape the window expects; no
XSS or id-injection path (credits are text, links https only); a cache hit spends no budget; memory bounded (~128 KB a
payload); the CHS notice verbatim, the JMA and HKO notices present; FES constants never leave the job (only the
encrypted file is cached, the plain one gitignored and deleted, no artifact upload, logs show counts only).

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P0-1 | P0 | A listed model point's window answered 404: the route looked in the stations only | FIXED: the route accepts `models` and reads the list first in a fresh process; test opens one (200) |
| P0-2 | P0 | Six non-tidal TICON-3 gauges (Toronto, Georgian Bay, Vaudreuil, three Elbe gauges above the weir) shown as tide stations | FIXED: a gauge needs M2+S2+N2+K1+O1 >= 5 cm; TICON-3's extra gauges are no longer added (no country, lakes typed "Coastal") |
| P1-1 | P1 | The FES workflow's test step could never pass (pytz, Flask missing): nothing would ever publish | FIXED: the workflow installs requirements.txt |
| P2-1 | P2 | 11 TICON gauges 2-3 km from an agency station (Rosslyn Bay, Kushiro, Port Chalmers ...) shown twice on two datums | FIXED: OFFICIAL_KM 3 |
| P2-2 | P2 | 353 TICON gauges in the USA (marsh monitors, river gauges, coordinate names) | FIXED: TICON's US gauges are left to NOAA |
| P2-3 | P2 | TICON-3 gauges credited as TICON-4 | FIXED: per-station credit (moot after P0-2: no TICON-3 gauge is listed) |
| P2-4 | P2 | JMA labelled "≈ LAT" with heights down to -0.34 m | FIXED: "tide-table datum (JMA; the lowest tides can read below zero)" |
| P2-5 | P2 | Queensland's yearly resource ids frozen in the snapshot: all 76 fail in late Nov 2027 | FIXED: a missing year is looked up at run time (package_show), kept a day; test |
| P2-6 | P2 | One failed read of the model list hid model points for an hour | FIXED: asked again 5 min after a failure |
| P2-7 | P2 | AVISO_PASS also keys the cache: a password change breaks the job; FTP in clear | FIXED: an undecryptable cache is rebuilt; the download is FTPS with certifi |
| P2-8 | P2 | A CHS fetch was 6 sequential requests holding a build slot (up to ~90 s) | FIXED: 31-day curve pieces (checked live: 31 days accepted, 32 refused): 3 requests, 1.5 s live |
| P2-9 | P2 | The FES job downloads all 34 global files before processing any (disk) | FIXED: one file at a time (`--add-file`), deleted after; test = whole-dir extract |
| P3 | P3 | ev.set() before the cache write (a waiter could get "busy") | FIXED: cached before the waiter is woken |
| P3 | P3 | A budget token spent when the build cap then timed out | FIXED: the cap first, then the budget |
| P3 | P3 | `_errors` never pruned | FIXED: expired entries dropped on each failure |
| P3 | P3 | "LAT, computed from the constants" shown for Queensland's own LAT | FIXED: datum "LAT (computed)" for computed, "LAT" for an agency's |
| P3 | P3 | The model text said "Predicted on this site" (the job predicts) | FIXED: wording |
| P3 | P3 | M1's speed differs from NOAA's; TICON's convention unconfirmed | RECORDED: small amplitude; NOAA's M1 is not used for TICON |
| P3 | P3 | TICON Hong Kong ~55 min early | FIXED beyond the finding: a GOT5.5 clock check drops 15 whole-hour clock errors (qc_ticon.py) |
| P3 | P3 | README's Norway LAT claim not reproduced at Stavanger / Oscarsborg | FIXED: README states both results |
| P3 | P3 | Licences: CHS non-profit only (owner to confirm); FES and GeoNames missing from the map credit | FIXED: map credit; CHS term for the owner |
| P3 | P3 | (Re-check by the author) LINZ's Chatham Islands tables were read as NZ time: 45 min late | FIXED: their own zone (Pacific/Chatham); now 0.7 min from TICON |
