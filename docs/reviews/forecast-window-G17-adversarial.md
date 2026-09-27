# Forecast window polish — G17 adversarial review (plan section 25, PR C) — 2026-09-26

Scope: `feat/forecast-polish` (d032ef2..74bf8ba, UI asset 1.6.0): the `<head>` early `/api/forecast` request, keyboard
resize on the corner handle, bring-to-front between the forecast window and the live-buoy panel, "Comb." in the compact
table. One fresh-context reviewer at HIGH (code, 12 mutants on a scratch copy, the test site desktop + 375x812).

## Findings: 0 P0, 0 P1, 0 P2, 5 P3 — fixed in UI asset 1.6.1 unless noted

| # | Finding | Outcome |
|---|---|---|
| P3-1 | A SWAN link on a non-SWAN station always wasted the early request (the head did not apply the loader's `normalise()`). | The head gets the SWAN station list and asks for GFS off it; the Node oracle applies `normalise`; cases added. |
| P3-2 | `?unit=constructor` (or a stored `toString`) passed the module's `UNITS[u]` lookup (inherited properties): the module sent it on, the head sent `US` (pre-existing). | `isUnit(u)` = exactly `US` or `Metric`; cases added. |
| P3-3 | A legacy POST (or `render=full` in a POST body) wastes the early request. | Accepted: the page never submits its form; the forecast shown is still correct. |
| P3-4 | Keyboard resize: `role="button"` implied Enter/Space; growth at the right edge shifts the window (as dragging does); each key press saves and resizes the charts. | `role="img"` + `aria-keyshortcuts`; the rest accepted (consistent with dragging). |
| P3-5 | Screen readers read "Comb.". | `aria-label="Combined"` on the abbreviation. |
| Tests | Surviving mutants: the minimised/phone key guard, `onResize` in `resizeBy`, the head's `catch`; no test for an early HTTP error + Retry. | Pinned: chart resize on a key, no resize minimised or in phone mode, the head attaches its catch at once, an early 503 shows Retry which fetches afresh. |

Verified by the reviewer: the early response is taken once and only for the identical query; any change before it lands
supersedes it (`seq`); errors give Retry; the head survives blocked storage and odd stored values; one request on a normal
load (started 307 ms vs 547 ms on production), none for `render=full`; z-order both ways with real live markers, the top
bar above both, phones unchanged (z 3500 rules later in the source); keyboard resize clamped, saved, charts follow; no
console errors.
