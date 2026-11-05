# G52 B: the satellite layer on the test site (plan section 81, step 5, reviewer B: the live site) — review record

Reviewer: one fresh agent (Fable), 2026-11-04 (23:56Z) to 2026-11-05 (00:35Z). Scope: the TEST site
https://wave-app-clean.onrender.com/ (branch `test` @ 81f13b6, overlay asset 2.23.2 as served at
`/overlay/overlay.js?v=2.23.2`), `ALT_OVERLAY` on; `SARSWELL` was off at the start and ON from 00:02Z (the Heights |
Swell switch was covered from then on). Data: `alt/v1/latest.json` builds 20261104T234627Z (11 chunks, newest reading
21:51Z), 20261105T000628Z (newest 22:07Z) and 20261105T001626Z (newest 22:53Z); `sarswell/v1/latest.json` build
20261104T235629Z (17 chunks, newest point 2026-11-04 17:23Z, CFOSAT + Sentinel-1C). Compared against
`docs/plans/section-81-satellite.md` (§81 and its base §45) and the owner's sheet `section-81-test-site-sheet.md`.
The G51 code review was read last. The client file is byte-identical between `feat/satellite` and `test` (only
`templates/index.html` differs by the three logo lines).

## Method

Headless Chromium through Playwright, read-only, a few page loads a minute. Desktop 1280x800 and a phone 375x812
(isMobile, hasTouch, an iPhone 17 UA), deviceScaleFactor 1. Every page collected `pageerror`, console errors, failed
requests and responses >= 400. The controller was read through `window.AllshoreOverlay._last` (field, satKind, satOn,
satLayer.count()/complete()/win, the stores' status / chunks / flat size), the dots through the obs pane's tile canvases
(opaque-pixel counts; a hover by moving the mouse to an opaque pixel; a touch by CDP `Input.dispatchTouchEvent`), the
panel through its text. Scripts and outputs: `/tmp/claude-0/-home-user-wave-app/ddff7080-5763-58b4-b5ba-6c83938be3ca/scratchpad/revB/`
(`d1`: the own entry, zooms, date line, poles, switching orders, the box with seek / play / model switch, tp / wind / Off,
reloads; `d2`/`d2b`: first-picture timing on a fast link and at 1.5 Mbps (CDP throttle, fresh cache), deep zooms on one
dot, an empty-water hover, metric, the frame-mode hover and the dot-vs-field colour, shark-speed frame rate with the box
on and off; `d3`/`d3b`: the pointer blocked, a chunk on 503, a chunk on 404; `d4`: the Swell switch, arrows, per-tab
memory, the box after Swell, a fast Heights/Swell/Heights flip; `d5`: a hollow dot, a "model —" dot, a `model_edge` dot,
a model switch while playing; `p1`/`p2`: the phone; `bg`: the own entry left open 14.5 min). Screenshots `*.png` beside them.

## Findings

| # | Sev | What I did | What I saw | What the design says | Suggested fix |
|---|---|---|---|---|---|
| 1 | P1 | Hovered a reading flagged `model_edge` (flag 2; 2,344 of 342,383 readings in the window, mostly along the Antarctic ice edge and coasts): SARAL at 68.5°S 97.0°W (`d5` hover-edge) | "10.8 ft · SARAL · Nov 3, 05:56 PM EST · model 0.6 ft (+10.2)". The model value is GFS-Wave's near-zero under-ice value beside an absent node; the hover presents it as a +10 ft model miss. Same for the frame mode (the field's drawn pixel there is the masked edge) | §45 A: the job records `model_edge` "when a neighbour was absent" so the client can tell an edge from a comparison; §81 purpose 2: "a check of the model" — the dot's colour vs the field is the comparison's premise, which does not hold at an edge | `_satReadout` (overlay.js ~5018): when `f.flags[j] & SAT_FLAG_MODEL_EDGE`, print "model 0.6 ft (edge)" and drop the difference (or "model — (edge)"); optionally draw such dots hollow too in `composeDots` (as `near_land`), since their comparison is as unreliable |
| 2 | P2 | Hovered a hollow (near-land) dot: Sentinel-3A at 18.5°N 155.1°W, 2.2 m, flag 1 (`d5` hover-hollow); read the panel | "7.3 ft · Sentinel-3A · Nov 4, 03:09 AM EST · model 6.2 ft (+1.1)": nothing says the reading is near land. The panel has no key for the hollow rim (head, scale, readings line, credit only). 26,300 of 342,383 readings in the window are hollow (7.7 %), so a user sees many rings with no explanation | §45 B: "`near_land` readings hollow (rim only)"; §81 known limitations: "Coastal readings are unreliable (hollow dots)" — stated in the plan, not on the page | `_satReadout`: append " · near land" (or "coastal") when `f.flags[j] & SAT_FLAG_NEAR_LAND`; `_renderSat` (~6992): one muted line "hollow = within ~45 km of land (less reliable)" under the readings line |
| 3 | P2 | Read the readings line on the own entry; listed the store's missions in the window (`d4` missions) | "Readings: last 24 h, newest Nov 4, 04:51 PM EST (2 h ago) · 9 satellites" while the window holds Jason-3 (51,741) AND "Jason-3 OGDR" (482) as two mission ids: 8 satellites, 9 ids. The same in every later pointer | §45 B: "· 7 satellites" counts satellites; §81 A: OGDR is "dropped as a duplicate of CMEMS's Jason-3 pass" — the same spacecraft | `_satLineText` (~4990): count distinct platforms, e.g. `short.replace(/ OGDR$/, '')` (or a `platform` key in the pointer's `missions[]`, set by `alt.py`), not distinct mission ids; a test with j3 + j3_ogdr both present |
| 4 | P2 | Tapped a dot on the phone (a 200 ms touch), then held 700 ms (`p1` tap-dot, `p2`) | A plain tap shows nothing; a 700 ms press shows "8.2 ft · SWOT · Nov 4, 04:36 PM EST · model 6.4 ft (+1.8)" (an arrow: "Swell 1: 4.2 ft · 14 s · from 317° (Sentinel-1C, 8 h ago)"), and the readout hides 1.5 s after the finger lifts. Nothing on the page hints at the long press | The owner's sheet: "the dots stay tappable (the hover text on tap)"; §45 B: "touch long-press unchanged" (the field readout's pre-existing 450 ms rule: `_bindReadout` ~6197 "a plain tap shows nothing"). The sheet and the behaviour disagree; a reader of the sheet taps and sees nothing | Either make a plain tap on a satellite mark show the readout (in `ts`/`te` of `_bindReadout`: a short touch that `hitAt`s a mark shows it, and keep the long press for the field), or fix the sheet's wording to "long-press"; and let a satellite readout stay longer than 1.5 s (its text is 60-90 characters; `te`'s `setTimeout(..., 1500)`), e.g. until the next touch |
| 5 | P3 | Hovered dots in ft and in m (`d2b`, `d4` agreement) | The difference is computed before rounding, so it can disagree with the two numbers shown: "2.5 ft · Jason-3 · … · model (frame …) 1.2 ft (+1.4)" (2.5 − 1.2 = 1.3); "2.9 m · Sentinel-3A · … · model 2.5 m (+0.5)" (2.9 − 2.5 = 0.4) | §81 decision 6: "the hover gives both numbers and the difference" — the reader checks one against the other | `_satReadout` ~5039: `diff = +u.f(swh).toFixed(u.d) - +u.f(mv).toFixed(u.d)` (the difference of the printed values) |
| 6 | P3 (once) | Routed the newest chunk to a persistent 503 and watched 150 s (`d3b`) | The rest of the picture drew ("loading 7 of 9…" at 1.8 s), the chunk was re-asked at 60 s and 121 s (the cooldown), the line read "loading 8 of 9…" the whole time with no word that a window is failing; once the route was lifted the next retry landed (30 s) and the line became "9 satellites". No exception | §45 B: "else cooldown + retry" (met); the line "loading 3 of 8…" is meant for chunks on their way, not for one that has failed three times | `_satLineText`: when `st.failed[key]` is a cooldown (a number) for a needed chunk, say "loading 8 of 9… (1 retrying)"; `SatStore` could count attempts per key so the wording can turn into "1 window unavailable" after N tries |
| 7 | P3 (once) | Routed one chunk (20261104T15) to 404 and re-entered the layer (`d3b` 3:404) | `complete()` true, "9 satellites", the picture silently lacks the 3-h window (296,334 readings drawn instead of 342,383); no hint anywhere | §45 B: "404/decode permanent" (met); nothing says the user should be told. A key the pointer names but the bucket lacks can happen briefly (a pruned superseded build) | `_satLineText`: when a needed chunk is `failed[key] === true`, say "· 8 of 9 windows" (or "1 window missing"); the 12-min recheck already brings a new pointer that may name a live key |
| 8 | P3 | Looked at the own entry at z2–z4 with the site's station layer on (`d1-z2.png`, `d1-z4.png`) | The site's station markers (cyan rings with a white centre, yellow discs) crowd every coast; the satellite layer's hollow coastal dots are also rings coloured by value (cyan at 1-3 ft), so near a coast the two kinds of ring sit together and read alike | §45 B: hollow = near land. The station layer is pre-existing; the clash is new | Consider a distinct hollow style for the satellite rings (a dashed rim, or the dot's rim in the value colour over a dark centre) in `composeDots` ~2822, or state the key (finding 2) |
| 9 | P3 | Read the Swell readings line (`d4`) | "Readings: last 24 h, newest Nov 4, 12:23 PM EST (7 h ago) · 2 satellites" under the head "Satellite waves (CMEMS, last 24 h)". Correct for "the 24 h before the newest point", but the newest point is 7 h (by design up to 31 h) behind the clock, so "last 24 h" reads as "the last day" to a visitor | §81 progress: "OWNER TO CONFIRM: the 'last 24 h' of swell are 5-31 h behind the clock" — open | If the owner keeps the window, say "24 h to 12:23 PM EST (7 h ago)" for swell in `_satLineText`; no code change if the owner accepts the wording |

No P0. No console error, failed request or >= 400 response from the site or the data host in any run (the only ones
seen were my own routed 503 / 404 / aborts and ArcGIS tile aborts on pans). Findings 1–5 and 8–9 reproduced on every look
(1–3 and 5 in several runs and pointers); 6 and 7 are marked **once** (one run each, `d3b`; within that run the 503
chunk was re-asked three times on the cooldown, as the finding says).

## Fix round (overlay asset 2.23.3, 2026-11-05)

| # | Fix |
|---|---|
| 1 | `_satReadout`: a `model_edge` reading reads "model 0.6 ft (edge)", no difference, in both the own entry and the frame mode. |
| 2 | `_satReadout` appends " · near land" (swell: ", near land" in the parenthesis); `_renderSat` adds the key line "Hollow dots: within a grid cell of land (less reliable)." under the readings line (`SAT_KEY`, heights only). |
| 3 | `_satLineText` counts spacecraft by the mission's short name with " OGDR" stripped: Jason-3 once. |
| 4 | `_bindReadout`: a plain tap (touchend before the 450-ms press) whose point `hitAt`s a satellite mark shows the readout for `SAT_TAP_MS` 4 s; a long-pressed satellite readout stays 4 s too (`satShown`, a closure flag; asset 2.23.4 after coast-build #584 / #585 failed on a `removeAttribute` the other suites' fake elements lack); the field keeps its rule. The owner's sheet says so. |
| 5 | The difference is computed from the two printed values (`toFixed` first). |
| 6 | `_satLineText`: "loading 8 of 9… (1 retrying)" while a needed chunk waits out its cooldown. |
| 7 | `_satLineText`: "· 8 of 9 windows" after the satellites when a needed chunk is a permanent failure (404 / undecodable). |
| 8 | Not changed (owner's call): the hollow style beside the station rings; the key line (2) names them. |
| 9 | Not changed (owner's open call on the swell window). |

Tests: `tests/overlay/sat.test.js` covers 1, 2, 3, 5, 6, 7 (the own-entry test); 4 is in the sheet's phone checks.

## Checked and fine (with the numbers seen)

- **The entry.** `#ovField` options ":Off, hs:Wave height, tp:Peak period, wind:Wind speed, sst:Sea surface temperature,
  sat:Satellite waves". Choosing it: mode obs, `obsPane` created (z 260), no player and no model select in the panel, the
  head "Satellite waves (CMEMS, last 24 h)", the scale "0 3 6 10 15 20 30 60+ ft" (metric: "0 1 2 3 4 6 9 18+ m"), the
  readings line, the credit line "Generated using E.U. Copernicus Marine Service Information (doi:10.48670/moi-00179)."
  (with ", doi:10.48670/moi-00178" after Swell), no stale warning (newest 2 h old; swell 7 h).
- **First picture.** Fast link, z4 over the Pacific, cold cache: the pointer at 264 ms, the first dots at 1.17 s (4
  chunks, 128,101 readings), all 9 chunks of the 24-h window at 1.59 s (339,946 readings; the flat store 352,952 rows,
  6.7 MB); the chunks requested newest-first (T21, T18, T15, T12, …), three in flight. At 1.5 Mbps / 150 ms (fresh
  context): the first dots at 4.05 s (the newest chunk: 8,402 readings), "loading 1 of 9…" … "loading 8 of 9…" stepping
  in place, complete at 26 s (346,366 readings). The readings line while loading: "Readings: last 24 h, newest Nov 4,
  05:07 PM EST (2 h ago) · loading 4 of 9…"; after: "… · 9 satellites" (but see 3).
- **Readings line and times.** "newest Nov 4, 04:51 PM EST (2 h ago)" = the pointer's `newest_utc` 21:51:27Z in the
  page's time zone (EST on this test page; the format is the page's `fmtTime`, with the date); the age rounds to the
  hour ("2 h" at 2.08 h); the newest moved 21:51Z → 22:07Z → 22:53Z across the three pointers seen.
- **Hover (own entry).** "9.6 ft · Sentinel-3A · Nov 4, 03:35 PM EST · model 8.1 ft (+1.5)"; "12.1 ft · Sentinel-3B · … ·
  model 11.8 ft (+0.3)"; a reading the job had no frame for: "10.3 ft · SARAL · Nov 3, 05:56 PM EST · model —"; metric:
  "2.9 m · Sentinel-3A · Nov 4, 03:35 PM EST · model 2.5 m (+0.5)" (see 5 for the rounding). Over empty water (no
  opaque obs pixel within 12 px) the readout hides. The same dot gave the same text after a 137x61 px pan and at every
  zoom from 8 to 12 centred on it (the dot keeps its place: the `i32` positions).
- **Zooms.** At a fixed centre (20°N 160°W): opaque obs pixels z2 125,887 (the map clamps to z2.32), z3 210,107, z4
  183,266, z5 136,443, z6 83,942, z7 55,064, z8 10,581, z9 2,508, z10–12 0 at that centre (no track in the 190-km view:
  data, not a bug; centred on a dot z10 3,070, z11 947, z12 240 px and the hover works). Tracks read as dotted lines
  from z4; at z2 the thinning keeps the picture light (stride 32).
- **Date line and poles.** Dots and hovers across 180° at z4 and z7 ("5.9 ft · SWOT …", "6.9 ft · SARAL … (+1.3)" at
  0.5°S 178°W), at 78°N (19.7 ft · SWOT · model 19.4 ft (+0.3) in the Barents Sea) and 72°S (9.2 ft · SARAL · model
  11.5 ft (−2.3)); nothing drawn over the ice sheets' land. The bucket walk and the slice scan agree (no gap at 180°).
- **Switching orders.** hs → sat → tp → sat → wind → sat → sst → sat → Off → sat: each return to sat showed the same
  339,946 readings at once, with NO new `/alt/` request (the samples stay in the store); leaving sat removed the
  obs canvases (0) and the 12-min timer; sst showed its own panel ("NASA MUR (1 km, daily)"); Off asked for nothing.
- **Credits.** Own entry: the map attribution ends "Overlay: Copernicus Marine" (26 visible characters; the link's
  title holds the full credit with both DOIs moi-00179 and moi-00178). Box on over wave height with readings in the
  window: "Overlay: NOAA GFS-Wave/GFS · Copernicus Marine · GSHHG" (ECMWF: "Overlay: ECMWF CC BY 4.0 · Copernicus Marine ·
  GSHHG"), the link's title "Satellite readings: Generated using …"; the " · Copernicus Marine" goes when the window
  holds 0 readings (frame 10 onwards) and comes back at frame 0, in the same landing as the dots.
- **The box under Wave height.** Row "☑ Satellite readings  within 3 h of the frame" under the scale, only on hs (none
  on tp / wind / sst / sat). On at frame 6 (valid 00:00Z): the window [21:00Z, 03:00Z], 1,854 readings, complete at
  once from the samples in hand; frame 0 (18:00Z): [15:00Z, 21:00Z], 81,681; frame 5: 9,114; frames 10, 24, 48, 208: 0
  and the note "no readings within 3 h of this time". The window's edges: `t0` inclusive, `t1` = valid + 3 h + 1 ms
  (a reading at exactly +3:00:00 is drawn). Play at 1x and 4x: `win.ref` stepped with every frame (07:00, 08:00, …).
- **Frame-mode hover.** "obs 6.1 ft · SARAL · Nov 4, 12:55 PM EST · model (frame Nov 4, 01:00 PM) 5.3 ft (+0.8)"; on
  ECMWF the frame time is ECMWF's. The colour premise holds: a dot with "+0.0" was rgb(70,214,136) on a field pixel
  rgb(70,214,137); "+0.3" → (102,219,108) vs (86,217,123); "+1.3" → (27,128,233) vs (35,87,208).
- **Model switch with the box on.** Paused: GFS-Wave → ECMWF kept the box, the window followed ECMWF's frame of the
  carried time (frame 8, 12:00Z Nov 5, 0 readings) and frame 0 (12:00Z Nov 4) held 89,423. Playing at 1x: the window
  cleared for one tick while the ECMWF manifest loaded (no stale dots), then resumed on ECMWF's frames with
  `satLayer.entry === layer.entry` in the manifest at every sample.
- **Shark speed.** 4x with the box on: 3.95 then 6.46 frames/s over 8 s; off: 5.45 (the differences are the frame
  downloads warming, not the dots: the third run with the box on was the fastest).
- **tp / wind / Off with the box on.** Each detached the layer (0 obs canvases, no timer, no `/alt/` request); back to
  hs brought the dots back without a download (89,423 at the carried frame).
- **Reloads.** Box on → reload: `allshore.overlay.v1` = `{"field":"hs","model":"ecmwf","playing":false,"sat":true,…}`,
  the box checked, the note by the restored frame ("no readings within 3 h of this time" at 11:00Z Nov 5). Sat chosen →
  reload: the entry restored, 339,946 readings. Swell chosen → reload: `{"field":"sat","satKind":"swell"}`, "Swell"
  pressed, 2,962 points.
- **Left open 14.5 min (own entry).** The pointer re-read at 716 s (`cache: no-cache`), the newer pointer (build
  …000628Z) adopted after its two changed chunks (T21, T18) landed; the drawn count never fell below 339,946 (431
  samples every 2 s; no flash), the window moved to [22:07Z − 24 h, 22:07Z] and the line to "newest … 05:07 PM EST".
- **Pointer unreachable.** Own entry: the error panel "Overlay unavailable: satellite readings cannot be loaded right
  now" with Retry, no exception. Box on hs: the note "readings unavailable", the box stays enabled, no credit; the
  landings re-asked once per 60-s cooldown (one request in 70 s of scrubbing); after unblocking the next landing
  recovered within 44 s ("within 3 h of the frame", 78,487), and the own entry then showed the full 346,366.
- **Swell (SARSWELL on from 00:02Z).** The switch "Heights | Swell" (`aria-pressed`) in the panel; Swell: the swell
  pointer and its 9 chunks of the 24-h window in 1.65 s ("loading 0 of 9…" … "· 2 satellites"), 2,962 points, mode
  `arrows`, store kind `swell`, the window [Nov 3 17:23Z, Nov 4 17:23Z]; arrows coloured by the first partition's height,
  longer for longer periods, pointing where the swell goes (a "from 295°" arrow points ESE); the hover "Swell 1: 3.5 ft
  · 12 s · from 295° (Sentinel-1C, 8 h ago)" (metric "1.1 m"); points with two partitions exist (e.g. CFOSAT 1.31/1.08
  m, 15.0/12.8 s, 253°/263°). Heights again: 346,366 dots, the credit line back to one DOI. A fast Swell/Heights/Swell
  flip (80 ms apart) ended on Swell, ready, 2,962, no error.
- **The box after Swell.** With Swell chosen on the own entry, the box under Wave height drew height dots (`mode
  'dots'`, store `alt`, 85,099 at frame 0) with the frame-mode hover; back on the own entry Swell was still chosen
  (arrows). No arrows over the model.
- **Phone (375x812).** No horizontal scroll in any state (scrollWidth 375). The sheet shows the full panel on entry; folded:
  "Satellite · newest Nov 4, 05:07 PM". The Heights | Swell switch fits (panel 175 px wide). The box row (307 px) and
  the note (one line, right edge 355 px) fit; the frame-mode tracks show over the field. Long-press readouts on a dot, an
  arrow and the field (see 4 for the tap).
- **Hollow dots.** Drawn hollow where flagged (26,300 in the window): the rings along the track south of Oahu and by
  Hilo at z8, filled dots offshore; the hover hits a hollow ring (see 2 for its text).
- **Memory / size.** The flat store 6.7–6.9 MB for the 24-h window (352k–363k rows), one layer, 24–48 tile canvases;
  no growth across the switching orders (the store is reused); the swell store 3,403 rows.
