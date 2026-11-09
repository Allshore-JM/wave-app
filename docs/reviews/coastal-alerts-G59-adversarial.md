# Coastal hazards (plan section 90): test-site verification, review G59 and its fix round

## Test-site verification (2026-11-08, Opus 5.5 HIGH)
Test site wave-app-clean (branch `test`), UI 1.29.0 then 1.29.1; headless Chrome 156 with its own profile (HL_GPU=1),
trusted input; scratch a90/v (v90.js parts, data90.py).

- Data (data90.py): NWS's active list 33 products = the 33 served; class, zones and onset identical; 67 zones; every
  served line = alert_coast.json's.
- Desk, US west coast at zoom 6: 952 lines and 952 halos, every style = an independent rule (0 wrong); one credit; the
  hover label (mouse-only stand-in, FINE=1) "High Surf Warning · South Central Oregon Coast"; the popup = the answer
  (zone ORZ021, events, description, NWS link), no element inside built from the text. Florida at zoom 8: red,
  orange and blue all right.
- Gate: no lines at zoom 4 / 4.5 / 4.9, all at 5 / 6 / 9; the credit follows. Toggle: off, kept after a reload, on.
- Notes: "loading…" while the server warms up, "unavailable" on a refused answer, back after a minute.
- Stand-in (DevTools Fetch, Oahu): warning, advisory, beach statement, flood statement, watch and a pending advisory
  (+75 s): 30 lines, colours right before and after the onset (the page judges each minute, no new answer).
- Phone 375x812: a tap 6 px off a line opens it; no hover labels; no sideways scroll.
- A forecast dot 5.7 px from a line opens its station, not the popup. A running map tool takes a click on a line.

### Found and fixed during the verification (UI 1.29.1, 8ee5960 + golden ffa9a27 + pin b9dcadb)
- V1 (P2) a click near two zones' junction opened the NEIGHBOUR's products (a point on the red Oahu North Shore line ->
  Koolau Windward; on blue -> Honolulu Metro): Leaflet's canvas gives a click to the last drawn path holding it, and
  the page's NearestCanvas ranked every line equal (no centre point). Lines now rank after every dot, then by their
  own distance to the pointer (LINE_RANK + AlertLine._lineDistance via closestLayerPoint). Re-checked: points on a
  line within 2-14 px of another zone's line open their own zone 20/20 (US west coast z6), 18/18 (Oahu z9), 20/20 (Oahu
  z10).
- V2 (P3) the phone popup ran 3 px past a 375-px screen: maxWidth = min(340, screen - 64), at least 200. Re-checked:
  21..359 px.
- Mutants of the two fixes: 7 of 7 killed (one after pinning 311 px literally).

## G59 (one fresh reviewer, Fable, on b9dcadb + the test site)
**0 P0, 1 P1, 4 P2, 4 P3.** Report: scratch g59/g59-report.md.

| # | Level | Finding |
|---|---|---|
| F1 | P1 | Foreign and across-water shores drawn as part of US zones: Windsor and Amherstburg ON (MIZ076), Wolfe Island ON (NYZ007), Sault Ste. Marie ON (MIZ087), Lake of the Woods north of 49 N (MNZ006), Tsawwassen / Boundary Bay / White Rock BC (WAZ310), Campobello / the St Croix estuary NB (MEZ030), Great and Little Thatch BVI (VIZ001), the beach south of the Rio Grande (TXZ455), Playas de Tijuana (CAZ043). The near rule joined a coast point outside every zone to a zone whose outer edge was within reach, across the water |
| F2 | P2 | "NEAR_KM 1.5" was 1.2-3.1 km of grid rings and took the LOWEST zone id in the first ring, not the nearest: lines ran up to 1-2 km past a junction (211 vertices in 107 zone pairs, 209 toward the alphabetically later zone) |
| F3 | P2 | The layer switched off and on within 2 minutes on a still map stopped the 2-minute refresh (no timer left) |
| F4 | P2 | Great Lakes islands had no coastline: MIZ097 Mackinac / Bois Blanc had no outline at all; Washington, Kelleys, South Bass, the Manitous, Madeline drawn without their islands |
| F7 | P2 | Lake Champlain's Vermont shore never drawn (VT not in the coastal states) |
| F5 | P3 | One malformed NWS feature failed the whole read (after 30 minutes every product would vanish) |
| F6 | P3 | Test gaps: ends-vs-expires, the popup closing when its zone's products end, the zone link's anchors |
| F8 | P3 | Every 2-minute answer rebuilt every line, and the ETag changed with every read (a 304 never happened); 2.4 s rebuild on a throttled phone in a hurricane-sized stress answer |
| F9 | P3 | Nits: 14 events, not 15; the README's "never on a visitor's request" (a request can start the background read); an open popup not re-judged at the minute tick; both world copies drawn across the date line |

Held up: the status rules on the live answer and on hostile inputs (cancel, replaced, ended, Test, odd references,
naive / odd-offset times, onset null, expires before ends); the served body = a local build of the same NWS answer byte
for byte; timeouts and bad answers keep the last list; popups and labels built as text (no element from NWS's text);
the junction clicks desktop + phone; the gate, the date line, the credit, the visibility memory.

## The owner's decision during the fix round (2026-11-08): only the open coast
Owner (with a Florida screenshot): "the rip current advisory in Florida shown the advisory at intercoastal waterway
boundaries as well as the actual coastline. It doesn't make sense at the intercoastal waterways, rip currents will only
occur at the ocean ... for this website being predominately surf focused advisories should only be shown at the actual
coastline, with no hazards shown at intercoastal waterways. The user can determine from the hazard text if it also
applies to other nearby waterways."

Built: a shore point is drawn only when a straight line from it, within 60 degrees of straight out to sea, runs 100 km
over water without touching land (land raster 0.005 deg; lines fanned every 5 degrees, first sample 0.8 km out, every
0.45 km). Every 5th point is tested, the rest take the nearest tested point's answer; a closed stretch under 1 km
between open ones is opened (a jetty, a cove) and an open stretch under 1 km is closed (a speck in a lagoon). The open
sea and the Great Lakes' open shores stay; bays, sounds, lagoons, harbours and the Intracoastal Waterway go.

- The final build (z_16ap26): 4,107,619 coast points, 1,895,259 in a zone, 498,574 on the open coast; **392 zones,
  6,258 lines, 450 KB** (was 682 zones, 12,644 lines, 1.85 MB). By state: AK 52, MI 46, FL 41, HI 35, CA 23, NY 19 ...
- Zones that lose their line (291): bay, sound, lagoon, river and inland-lake zones as intended: Puget Sound, San
  Francisco Bay, Chesapeake Bay, Long Island Sound, Lake Champlain (NY and VT), Lake Tahoe and the Salton Sea, the
  Texas mainland "Coastal ..." zones (the barrier-island zones stay), mainland Brevard, Juneau and the Inside Passage.
- Kept zones lose their inner shores: Pinellas 609 -> 77 km, the New Jersey coastal zones to 10-22 % of their length,
  CAZ006 San Francisco 60 -> 16 km.
- Pictures (scratch ocean90/open_coast_check.png, open_coast_check2.png, sent to the owner): Brevard keeps the ocean
  beach only (the Indian River Lagoon and the ICW gone); Oahu: Pearl Harbor and inner Kaneohe Bay gone, the outer coast
  kept; Maui north, Niagara (the river dropped, Lake Ontario kept), New Jersey, the Tampa Gulf beaches, the Outer Banks,
  Pensacola and Lake Erie's western shores kept.
- New `--check` probes: Cocoa Beach (FLZ747) and Cape Hatteras (NCZ205) must have a line; the Indian River Lagoon's
  mainland shore at Cocoa and Pearl Harbor must have none; the F1 places (Windsor, Wolfe Island, Tsawwassen,
  Campobello, Great Thatch, Playa Bagdad, Playas de Tijuana) must have no line of the US zone within 1 km. Zone bound
  330-500.

## Fix round (feature branch feat/coastal-alerts: 5494ba9 builder, 9cc9024 data, 01976bf server + page UI 1.29.2, af6dc0b golden)
- F1: a coast point outside every zone joins a zone only when the way to that zone's edge crosses no land (a fine
  0.001-deg water test along it) and does not run along the coast (the cos test against the coast's own direction);
  the far probes above. On the final build every F1 place has no line.
- F2: the zone is the one whose outer edge is NEAREST in kilometres (cos-latitude corrected), within 1.5 km.
- F3: sync() schedules the next ask from the due time when the layer comes back on (fetchTimer re-armed).
- F4: Great Lakes islands from the lakes-v1 set (the rings lakeoutlines-v1 lacks): MIZ097 Mackinac now has 21 km;
  WIZ022 Door keeps Washington Island.
- F7: superseded by the owner's open-coast rule (Lake Champlain gets no line on either shore: no 100-km reach of open
  water there).
- F5: each NWS product parsed on its own (`_alert()`); a malformed one is counted and left out, never failing the read;
  type checks on event / id / affectedZones / geocode / parameters.
- F6: tests for ends-over-expires, a malformed product (7 shapes), an exception inside one product, the popup closing
  when its zone's products end, the zone link only for a whole id.
- F8: the page redraws only when the answer changed (a signature of the products and the zones' lines + count of
  freshly decoded zones); the ETag is the md5 of the products and zones without the read time, so an unchanged list
  answers 304.
- F9: README wording ("a visitor never waits for NWS: a request can at most start the background read"); the open popup
  follows the minute re-judge (rebuilt only when its text changes) and closes when its zone's products end; the 14
  events are the owner's list. The two world copies across the date line: accepted (a cost only).
- Tests: pytest alerts 38 + alert_coast 23 (parametrised cases included) + page / flag; Node alerts 19; JS mutants 7 of 7 killed. Full suites:
  pytest 2,636 passed + 1 skipped (the two known load-sensitive tests pass alone), Node 1,220.

## Test-site check of the fix round
test @ db5d69c (cherry-picks of 5494ba9, 9cc9024, 01976bf; README the test branch's own; the golden stays the test
branch's), UI 1.29.2 served from 00:51 UTC 2026-11-09 (alerts.js and forecast.js = the commit, 1.29.1 404). Headless
Chrome 156 with its own profile (HL_GPU=1, never signed in), trusted input; scratch a90/v (v90.js, v59fix.js,
diag_junc.js, data90.py).
- Data: NWS's 33 active products -> 32 kept by an independent reading (one names only zones with no open coast) = the
  32 served; class, zones and onset identical; 59 zones, every line = alert_coast.json's. /healthz: 32 alerts, 392 coast
  zones, zones named without a coast line TXZ245 / 342 / 343 / 344 / 346 / 347 (Texas mainland coastal zones, as the
  rule intends).
- 304 (F8): the same ETag across the server's 2-minute re-read; `If-None-Match` answered 304.
- Desk, US west coast z6: 690 lines and halos, every style = an independent rule (0 wrong); popup = the answer
  (ORZ021); Florida z8 red / orange / blue right. Gate: none at zoom 4 / 4.5 / 4.9, all at 5 / 6 / 9.
- Junction clicks (points on a line within 2-14 px of another zone's line): west coast z6 20 of 20, Oahu z9 20 of 20,
  Oahu z10 20 of 20 (stand-in). The first desk run counted 3 of 20 because its points lay within a forecast dot's touch
  reach (this laptop reports a coarse pointer: a dot's reach is ~12 px, and dots rank before lines by design): those
  taps opened 46097 / 46212 / 46030 as designed; the check now skips points within 13 px of a dot (diag_junc.js).
- Stand-in (Oahu, six products incl. a pending advisory): 40 lines, colours right before and after the onset.
- Refresh (F3): layer off/on 38 s after the first ask; the next ask came on time at +120 s (before the fix: none).
- Redraw (F8): an unchanged answer kept all 690 line objects (0 rebuilt). Chrome adds the revalidation header below
  DevTools' request event, so the browser's 304 is not visible there; the server's 304 is shown above.
- Popup (F9): an open popup on a pending High Surf Advisory read "(not yet in effect) From Sun 3:00 PM"; after the onset,
  with no click, "In effect until Mon 2:59 PM" and the line orange. A popup on a warning that ended while open closed by
  itself and the line went.
- Phone 375x812: a tap 6 px off a line opens it (San Francisco: Beach Hazards Statement + Coastal Flood Advisory), the
  popup inside the screen. A forecast dot 5 px from a line opens its station (46240); a running map tool takes a click
  on a line (223 mi measured). Fresh page: no problems.

## Re-check of the fix round (one fresh reviewer, Fable, on af6dc0b): 0 P0, 1 P1, 2 P2, 4 P3
Report: SCR/g59/recheck/recheck-report.md (scripts and plots beside it).
- R1 (P1) open surf coast lost: GSHHG's headlands lie 0.05-0.6 km outside NWS's coarse zone polygons; the new
  along-coast refusal left those points without a zone, the open stretches between them fell under 1 km and were
  closed (Poipu, Hapuna, the Kona coast, Molokai's north cliffs, Keanae, St Thomas, Ponce, Vieques, Marquette, Pea
  Island): ~180 km of open Hawaii coast in 88 stretches of 1 km or more.
- R2 (P2) bays whose mouth lines up with the shore pass the single-line test: SF Bay (Berkeley, Sausalito), Biscayne,
  Hatteras Island's Pamlico side, Kaneohe Bay, Green Bay, Cape Cod Bay, Hampton Roads, Delaware Bay, Pamlico / Neuse,
  CT Sound shore, Port Townsend.
- R3 (P2) the README's claims did not match the file (SF Bay / Long Island Sound zones drawn; Champlain / St Clair
  products draw nothing).
- R4 (P3) 8 of 15 mutants survived the builder's tests; PROBES had no point outside Oahu / CA / OR / FL / NC.
- R5 (P3) islands facing a nearby shore lose their lee coast (Catalina's lee, Santa Cruz Island's north shore).
- R6 (P3) page nits (a text-only change rebuilds every line; a fetch after the layer is off schedules a timer; the
  refreshPopup path untested). R7 (P3) a 2.7-km gap at Pea Island (R1's mechanism).
- Owner's decisions (AskUserQuestion): bay mouths = a width test (open water must also be wide near the shore); lee
  shores = keep them out (islands facing another shore 30-40 km away stay unoutlined; Avalon and Lahaina keep theirs).

## Fix round 2 (feat/coastal-alerts @ b264db6; test @ 4dab31f)
- R1: `near_ok` refuses along the coast only past NEAR_SHORE_KM 0.5 km (nearer, the edge is the zone's own shore);
  `fill_holes`: a zone-less stretch under HOLE_KM 1 km between two points of one zone takes that zone.
- R2: `open_sea`'s width test: lines 2.5 and 5 km to either side of the open line must be over water from WIDE_FROM_KM
  10 to WIDE_TO_KM 25 km out. Measured variants on 6 cells (184 Y / 99 N points): none 13 missed / 24 drawn; 10-100 km
  14 / 9 (lost Ventura pier, Avalon, Bodega, Madeline Island: the Channel Islands' gaps); 10-25 km 13 / 10 (chosen);
  5-25 km 14 / 7 (lost Old Orchard, Hanalei); a 10-deg wedge 14 / 15.
- Kaneohe Bay: the barrier reef is not land in GSHHG, so the near fix joined its inner-shore pieces into one line ->
  `SHELTERED` (a hand polygon: Kaneohe Bay inside its reef; Mokapu's North Beach, Kailua, Kaaawa outside it stay).
- R3: README rewritten (the two tests, wide bays still drawn, the reef list, Champlain / St Clair draw nothing,
  counts). R4: tests for near_ok's 0.5-km rule and |cos| 0.6 / 0.8, fill_holes, the width test (a mouth at 10 km
  closed, a 30-km channel open, rocks 2.5 km aside closed, a 12-km mouth open, nothing near looked at sideways),
  RAY_FIRST_KM and FAN_STEP, sheltered(), OPEN_EVERY, TANGENT_PTS, the eligibility cut, NEAR_WATER_T, the shore cells:
  all 13 mutants (the re-check's 8 + the fix round's 5) killed. PROBES +11 shore points (Poipu, Hapuna, Kona,
  Molokai, Keanae, Ponce, Presque Isle, Pea Island, Newport RI, Mokapu, Avalon); NO_LINE +7 (Berkeley, Sausalito,
  Newport News, Stamford, the Pamlico River, Heeia, Kahaluu). R5: the owner's call (kept out). R6: recorded.
- Build: 361 zones, 3,383 lines, 425 KB (was 392 / 6,258 / 450 KB). `--check` clean.
- Results: the re-check's 66 named points: every R1 loss back within 0.7 km except Puako 2.24, Mahukona 1.51, Magens
  0.74, Marquette 0.85, Whitefish 1.58 (probe placement: each lies 0.7-2 km from the coastline itself or across a
  point); sheltered shores now 6.5-50 km from any line (Berkeley 16.5, Stamford 45.9, Pamlico River 50, Heeia 4.7,
  Kahaluu 7.4). The reviewer's 457 probes: lost 0; drawn-but-sheltered 6 left, all owner awareness below; 39 never
  within 0.7 km before or after, every one the probe's placement (measured: a probe on a barrier island's sound side
  or inland of the GSHHG coast, the line on the ocean side 1-2 km away; the Great Lakes points against the lake shore).
- Suites: pytest 2,648 + 1 skipped, Node 1,220.

## Test-site check of fix round 2 (test @ 4dab31f, served from 02:40 UTC 2026-11-09)
- /healthz alerts coast_zones 361. The site's answer against NWS's /alerts/active read and judged by independent code
  (data90.py): 33 NWS products, 31 kept (2 replaced), 31 served, the same classes / zones / onsets, 57 zones named and
  carried, every zone's lines = the new alert_coast.json.
- Headless Chrome 156 (own profile, trusted input): desktop popup = the answer (no HTML), 20 of 20 junction clicks on
  the West Coast open their own zone, Florida z8 293 lines all = the expected colours; phone 375x812 a tap 6 px beside
  San Francisco's line opens its popup inside the screen, no sideways scroll; the measure tool takes clicks on a line;
  fresh page no console problems. (The phone script first tapped 6 px beside a vertex where the Ocean Beach tide
  gauge's icon sits: the tide window opened, by design; the script now checks the tap point is open map.)


## Owner awareness
- Only the open coast is drawn: a product on a bay-only zone (Chesapeake, Puget Sound, Long Island Sound, SF Bay, the
  Texas mainland coastal zones, inland lakes) draws nothing; Lake Champlain and Lake St Clair products draw nothing;
  the product's text stays in NWS's products.
- Wide bays still pass both open-coast tests and keep a line: Green Bay's shores, Cape Cod Bay, Saginaw Bay (Bay
  City), Hampton (through the Chesapeake's 18-km mouth), Sandusky Bay, Biscayne Bay's mainland, Hatteras Island's
  narrow sound side at Rodanthe, the Matanzas River at St Augustine, Duluth harbour and Honolulu Harbor's mouth.
- Reef-sheltered water is a hand list (`SHELTERED`: Kaneohe Bay inside its reef today); other reef lagoons are judged
  by the land tests only.
- Islands facing another shore 30-40 km away keep no lee line (Catalina's lee, Santa Cruz Island's north shore);
  Avalon and Lahaina keep theirs.
- Lines are not keyboard-focusable; touch laptops get no hover label (the touch targets instead); islets outside every
  NWS zone are not outlined.
- On touch screens a forecast dot takes a tap within about 12 px of its centre, also where a hazard line passes
  through that circle (dots rank before lines by design, so a tap meant for a forecast point is never lost to a line).
  A tap a little further along the line opens the hazard.
