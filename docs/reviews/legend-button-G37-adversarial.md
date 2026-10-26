# Section 54: the legend as a button (Legend, Tools, Settings in one row): review record

Plan section 54 (plan file swirling-spinning-snowflake.md). Owner decisions (2026-10-26): the legend starts closed
everywhere; a dot on the key while a legend note exists, the note's text in the key's label; quick hover labels (mouse
hover and keyboard focus, never touch); order Legend, Tools, Settings. Step 2 (owner, with the go): the row below the
longitude labels ("it looks funny having the longitude offset to avoid the buttons").

## Step 1: the build (Opus 5.5 HIGH)

`feat/legend-button` @ dda0234 (code) + 1ec21aa (golden), UI 1.26.0. The key button (#legendBtn, an SVG key, a
`.legend-dot`) first in the top-right row; Leaflet's layer control moved into its panel (#legendPanel); the gear's toggle
(forecast.js `createSettings`) for it; the corner measure counts the corner's own controls only (the row and the tools
bar); `syncLegendNote()` from every note painter; CSS quick labels from `data-tip`. The overlap sweep, extended to the new
corner sizes, found the wind window starting above the forecast window's 96 px top at 1280-1365 px with the overlay panel
(the forecast window hidden whole with all four windows open): from 1280 px the wind window starts no higher than 128 px
(`--ww-top`). The sweep's rectangle subtraction keeps maximal pieces (it had split a free 60 x 34 px corner into two
thin strips).

## Step 2: test-site verification (Opus 5.5 MAX)

Built during the step (owner's request and two findings), each with tests and a golden of its own:
- 46a9dee: the row 26 px down (`margin-top`), below the longitude labels at the map's top edge (4 px, ~13 px tall): a
  label keeps its place instead of being pushed down around the buttons. Same place with the gridlines off.
- 93cd6f9 (found): the SVG buttons (tools, legend) sat in a host 3 px taller than the button (the line box's descender),
  so their panels opened 7 px below them and the gear's 4 px (the tools menu since section 29): `vertical-align: top` on
  `.icon-btn`. The corner measures 64 px (70 on phones).
- b73d1d1 (found): in forced colours (Windows High Contrast) the dot took the button's colour and vanished: the system
  highlight colour with a Canvas ring.

`test` @ a5179c7 (cherry-picks; README the test branch's own). Headless Chrome 155 (own profile, never signed in, GPU),
trusted mouse, touch and keys; scripts scratch `legend/v` (v.js parts row / legend / tips / note / corner / places / nojs /
console; sheet.py).

- Deploys: 18:32:07 UTC (11 polls across the switch, 0 failed), then the two fixes; the served UI files = the commit,
  1.25.9 answers 404.
- The row at desktop 1280x800 and 1920x1080, tablet 1024x768 (touch), iPad 820x1180 (touch), phone 375x812 (touch) and
  phone sideways 812x375 (touch): legend, tools, gear in that order, one row, 26 px from the map top, 10 px from its edge;
  38-px buttons (44 on the phone); the legend closed on load with the layer list inside its panel; `--map-topright-h`
  = the row's bottom (64 / 70 px) and `-w` = its width + margin (136 / 154 px); no longitude label pushed down on the
  right half, the labels over the row's columns at the edge above it; no sideways scroll; gridlines off: the row and the
  measure unchanged.
- The legend (desktop by mouse, phone / sideways phone / tablet by touch): a click or tap on the key opens it (aria-expanded,
  the corner raised); the corner measure unchanged while open; the panel 4 px under the key, right edges aligned, inside
  the map (8 px above its bottom; sideways phone max-height 246 px); the focus to the first layer checkbox; Tools and
  Settings stay clickable beside it; Tide stations unticked and ticked again, the legend open throughout; a press on an
  empty map point closes it and opens nothing; one panel at a time (legend, tools, gear, legend); the key again closes
  it. Keyboard: Enter on the key opens it with the focus on the first checkbox, Space toggles Forecast points, Tab stays
  in the list, Escape closes it with the focus back on the key and does not reach the forecast window.
- Quick labels (desktop): mouse hover on Legend, Tools, Settings shows "Legend", "Tools", "Settings" at once (read straight
  after the move: no delay, no transition); gone with the pointer; none on the key while its panel is open; the Tools
  label shows over the open legend; keyboard focus (Tab) on Tools and Settings shows theirs. Settings' label ends at the
  button's right edge, the others centred (max 220 px). Phone: no label on a tap, open or after closing (no hover, not
  focus-visible).
- The dot (desktop; the live list held and the tide list refused by DevTools Fetch at Oahu z9.5): the dot on the closed
  key; label and aria-label "Legend · Live buoys: loading…; Tide stations: unavailable"; open: the dot hidden, the notes
  under their layers; the live list released: "Legend · Tide stations: unavailable" with the dot; the tide list let
  through and asked again on a real drag 31 s later (TIDE_LIST_RETRY_MS): no dot, the label back to "Legend". Normal loads:
  4 first visits and 8 return visits (desktop and phone) with the dot sampled every 25 ms for 8 s: never shown (the lists
  land before the notes' 300-ms delay).
- The corner and the windows: the tools menu open: the measure unchanged; a tool's bar: the corner grows to it (137 /
  158 px) and back when it closes. 1280 and 1920: the expanded forecast window in the wide box from 96 px, its right edge
  152 px from the map's (136 + 16), its left 16 px right of the top-left controls. Maximised at 1280, 1920 and the tablet:
  top 72 px (the row + 8), its own buttons and the row's all clickable, and the legend opened over it with every checkbox
  reachable. The wind window from a real click on a flag: top 128 px at 1280 and 1920 (the floor), 72 px on the tablet,
  its buttons and the row's clickable. Phone: expanded windows are full screen above the row (by design, section 25 / G16).
- Place names (Europe z5 desktop and phone, New York z7): none under the open legend (Riga and Vilnius sat in its area
  before it opened and were laid out again).
- Top-left: the longitude label under the logo is hidden (the overlay control below it), with the overlay on and off:
  no label is pushed down anywhere.
- No JS: the legend's host and the tools hidden, the settings in the page head.
- Console: fresh page targets (desktop and phone) through a round of the three panels and every checkbox: clean.
- Forced colours: the labels keep a dark box with white text; the dot (fixed) in the highlight colour.
- The final build (test @ a5179c7, live 18:56:01 UTC): the row (desktop, phone, sideways phone), the legend (desktop,
  phone), the labels (desktop, phone) and a fresh-page console run again: 76 checks, 0 failed. Suites on the feature
  branch: pytest 1,553, Node UI 523. Cleanup: the test and production origins' storage emptied, Chrome stopped and its
  profile removed (no sign-in, no account entry).

Owner awareness: the dark label box is faint over the darkest ocean imagery (its white text reads); on phones an open
legend covers part of the overlay selector at the top-left while it is open (a dropdown, like the gear's panel); the
SVG key and ruler are drawn, the gear is a text glyph (unchanged).

## Step 3: G37 (one fresh reviewer, Opus 5.5 HIGH)

On `2ccfa41..43858b7` and the test site @ a5179c7 (report scratch `g37/r/g37-report.md`; probes `g37/h`). **0 P0, 0 P1, 2 P2,
4 P3.** The author confirmed each by code reading and the reviewer's screenshots.

- **F1 (P2)** The quick labels are cut off under a maximised forecast window (72 px; the label runs 70-89 px) and under any
  window whose top is above ~92 px (the tablet's wind window): the labels belong to the top-right corner (z 1000) and the
  windows lie above it. Worst for the key, whose label carries the note while the legend is closed.
- **F2 (P2)** One panel at a time fails by keyboard: Enter on the key, Tab to Tools, Enter -> the legend and the tools menu
  open together (the menu covers the list); Escape closes the menu only. Each toggle closes only on an outside pointerdown
  (forecast.js createSettings, tools.js setMenu). The gear + tools pair had this before; the legend panel is the one that
  lies under the others.
- **F3 (P3)** The dot and the key's label report the Live buoys note while the Live buoys layer is unticked (tide and wind
  clear theirs when their layers are off); a failing live list keeps the dot on every load for a viewer who turned the layer off.
- **F4 (P3)** Escape on the key: (a) the legend open, focus on the key: Escape closes a window behind instead (createSettings
  listens on the panel only; the gear has the same pattern since section 26); (b) a map tool running, focus on the key:
  Escape does nothing (tools.js ownsKeys knows the tools button and the closed gear, not the key).
- **F5 (P3)** The legend panel is always exactly its 180-px min-width (absolutely positioned in a 38/44-px host); the
  comment's "as wide as the list" and the max-width do nothing. Nothing is cut off (notes wrap).
- **F6 (P3, tests)** Survivors: createSettings' Escape `stopPropagation` removed (the "does not reach the window" assertion
  cannot fail: the window boots minimised); the legend dropped from `panels`; syncLegendNote counting only the live note
  (only the byte golden notices the last two).

Held up: suites (pytest 1,553, Node 523); the `--ww-top` floor (mutants at 64-110 px fail the sweep; 112 is the threshold, 120
ships); the sweep's new `minus()` can only under-report free space; C1 (the list rebuilds inside the panel, no Leaflet
height or scrollbar), C3, C5 (no TDZ, every note path through a painter), C7 (places, graticule, tools, overlay), C8, C10.
Not checked: real iOS/Android, real screen readers, real Windows High Contrast.

### Fix round scope (step 4, UI 1.26.1)
1. F1: raise the corner (z 2600) while a quick label shows: `:has(.icon-btn[data-tip]:hover)` under `(hover: hover)` and
   `:has(.icon-btn[data-tip]:focus-visible)`; test-site check: maximised window + hover / keyboard focus on each button,
   the label wholly visible.
2. F2: one rule for the three toggles: when one opens, the other two close (createSettings gains a shared registry; the
   page's tools menu joins it through an event tools.js dispatches when its menu opens, and closes on the others').
3. F3: the live note counts for the dot and label only while the Live buoys layer is on; syncLegendNote re-run on the live
   layer's overlayadd / overlayremove.
4. F4: createSettings closes on Escape with the focus on its own button too (stopPropagation); tools.js ownsKeys gives the
   key with its panel closed the tool's Escape, like the gear.
5. F5: the comment corrected (a fixed 180-px panel; the notes wrap).
6. F6: the window test expands the window before Escape in the panel; behaviour tests for the legend in the corner's raise
   and for the tide and wind notes reaching the dot; mutants re-run.

## Step 4: the fix round (Opus 5.5 HIGH)

`feat/legend-button` @ f8648a6 (code) + 3526703 (golden), UI 1.26.1. Every item of the scope:
- F1: `.leaflet-top.leaflet-right:has(.icon-btn[data-tip]:hover)` (under `(hover: hover)`) and `:has(...:focus-visible)` lift
  the corner to z 2600 while a quick label shows; the label takes no pointer, so nothing under it stops being clickable.
- F2: `createSettings` sends `allshore:panelopen` {panel} when it opens and closes on another panel's word; the tools
  menu (tools.js `setMenu`) sends and hears the same event. A keyboard opening now closes the other two.
- F3: `syncLegendNote` counts the live note only while `map.hasLayer(liveBuoyLayer)`; it runs again on the live layer's
  `overlayadd` / `overlayremove`.
- F4: Escape on an open panel's own button closes it and stops there (gear, legend, tools menu); tools.js `ownsKeys`
  gives the map tool the key's Escape while the legend's panel is closed, as for the gear.
- F5: the width comment corrected (`width: 180px`, the notes wrap).
- F6: the window test opens a live window behind the legend before Escape (`closes` stays 0); new tests for the keyboard
  path (two toggles, the tools menu's event, Escape on the key with the panel open and closed), the tools menu (its word,
  another panel's word, its button's Escape, the key's Escape with a tool running), the notes (tide and wind notes reach
  the key; the live note only with its layer on), and page pins for the panels list, the `:has` rules and the listener.

Suites: pytest 1,553, Node 1,028 (UI 528). Targeted mutants on a scratch copy: 19 of 19 killed (both `stopPropagation`s,
the button Escape and its open guard, the `panelopen` listener / its own-panel guard / the send, in both modules;
`ownsKeys` dropping or always owning the key; the live filter; the listener; the panels list; both raise rules).

## Test-site check of the fix round (Opus 5.5 MAX)

`test` @ 6016e5b (cherry-picks of f8648a6 and 3526703, both clean), UI 1.26.1. It was live from 20:53:03 UTC: 10 polls
across the switch, none failed, the last 1.26.0 answer at 20:52:52. The nine UI files hash to the pin (597331ef…) and the
1.26.0 URLs answer 404. The site files are the feature branch's apart from the test branch's three logo lines.

Setup: headless Chrome 155 with the GPU, its own profile, never signed in, with trusted mouse, keys and touch. The scripts
are scratch `legend/v4/g37.js` (parts f1, f2, f3, f4a, f4b, cost, cover) and the step-2 parts of `legend/v/v.js`. Each
finding was reproduced on 1.26.0 first, then checked on 1.26.1.

- **F1 (labels over windows).** The label takes no pointer, so the check reads its pixels from a screenshot.
  - On 1.26.0 the band under each button was 100 % the window's white, by hover and by keyboard focus. That held over
    the maximised forecast window at 1280 and 1920 (top 72 px), and over the wind window on the tablet by keyboard
    focus. Only a 2-px navy sliver showed above the window's edge.
  - On 1.26.1 the band is 64-70 % the label's navy and 22-26 % its white text, at all three sizes and for all three
    buttons. The corner sits at z 2600 while a label shows and returns to 1000 when the pointer or the focus leaves.
    The window's minimise button is on top again.
- **F2 (one panel at a time by keyboard).** Enter, Tab and Shift+Tab only.
  - On 1.26.0 the legend and the tools menu opened together, then all three panels did; five checks failed.
  - On 1.26.1 one panel is open at every step: legend, tools, gear, tools, legend. Escape in the legend closes it with
    the focus on the key. A click on Tools with the gear open by keyboard opens the menu only.
- **F3 (the live note and its layer).** The live list was held by DevTools Fetch.
  - On 1.26.0 the dot and "Legend · Live buoys: loading…" stayed with Live buoys unticked.
  - On 1.26.1 there is no dot and the label and aria-label read "Legend". Ticked again, the note is back. Once the list
    lands, the dot goes.
- **F4a (Escape on an open panel's button).** A live window (51201) and the expanded forecast window were open behind.
  The legend, the gear and the tools menu were each opened by a click, then Shift+Tab moved the focus to the panel's
  button.
  - On 1.26.0, Escape left the panel open and minimised the forecast window (the window in front).
  - On 1.26.1 the panel closes, both windows stay, and the focus stays on the button.
  - A second Escape, with the panel closed, follows the page's own rule (focus, then the window in front, then wind,
    tide, live): the forecast window is minimised and the live window stays. My first run expected the live window to
    close here, as in the 1.26.0 run. That was my error: on 1.26.0 the first Escape had already minimised the forecast
    window.
- **F4b (a map tool and the legend key).** The distance tool had two points.
  - On 1.26.0, Escape with the focus on the closed key did nothing; four checks failed.
  - On 1.26.1 the sequence is:
    - with the key's panel closed, Escape clears the points;
    - with the panel open, Escape closes the legend and keeps the points;
    - with the panel closed again, Escape clears, then closes the tool.
  - With the tool running, the tools menu open and the focus on Tools, Escape closes the menu and keeps the points. The
    next Escape clears them. The tool's capture-phase handler stays out while the menu is open.
- **Regression.** The step-2 checks all pass:
  - row at 1280, on a phone and on a phone sideways;
  - legend at 1280 and on a phone, including one panel at a time by taps;
  - labels at 1280 and on a phone (never on touch);
  - corner at 1280 and on a tablet;
  - place names and the no-JS page: 92 pass, 0 fail.

  The dot round (live list held, tide list refused, 31-s retry) passes 5 of 5; its one console error is the 503 the
  script injected. The console on fresh pages at 1280 and on a phone is clean.
- **Cost.** 30 sweeps over the row (90 hovers onto buttons) gave 360 style recalcs and 85 ms in all, under 1 ms a
  hover. That includes the label's own show and hide, which 1.26.0 had too. Moves over the map gave 142 recalcs and
  24 ms.
- **Suites on the test branch.** Node 1,027; pytest 1,155 passed + the 5 failures the test branch always has (its logo lines vs the page golden x4, the numpy-import test).
- **Cleanup.** The test and production origins' storage was emptied, Chrome closed by DevTools with no process left, and
  its profile removed. The sign-in check found nothing.

Owner awareness:
- A window dragged under the row: hovering the part of a button that shows lifts the whole row over the window, then it
  drops back when the pointer leaves. While hovered, the few pixels where a button overlaps the window's header belong to
  the button.
- `:has()` needs Chrome / Edge 105, Safari 15.4 or Firefox 121. An older browser keeps 1.26.0's behaviour (the label
  under a window).
- The sheet is in scratch `legend/v4/legend_g37_fixes_test_site.jpg`.
