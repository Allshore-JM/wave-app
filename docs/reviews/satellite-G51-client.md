# G51 A: the satellite layer client (plan section 81, step 5, reviewer A: code) — review record

Reviewer: one fresh agent (Fable), 2026-11-04, on `feat/satellite` @ e6a9495: the client in `static_overlay/overlay.js`,
the page, `app.py`, the CSS, `tests/overlay/sat.test.js`. Probes under the reviewer's scratch (`p1`-`p6`: the latched
re-read, the deferred adoption, the credit order, two dots 1.5 px apart, the rebuild cost, the box after a 503). The SST
path, the page gating, the asset pin and the decoder's bounds were verified unchanged / sound. Reviewer B (the test
site) runs once the data is live and the flags are set.

| # | Sev | Finding | Fix (overlay asset 2.23.1) |
|---|---|---|---|
| 1 | P0 | `hitAt` reported the nearest dot CENTRE: where tracks cross, the cursor over an older dot's centre inside a newer dot painted on top read the older reading (not the colour under the cursor) | `SatIndex.nearest` takes an `inside(j, dx, dy)` test: the top-most drawn mark holding the cursor (a dot's disc; an arrow's shaft and head) wins, the nearest centre within the slack only when none does; test with two dots 1.5 px apart at z8 |
| 2 | P1 | Off during a pointer RE-READ latched `loading` (its catch saw another owner): no pointer read again in the tab, the recheck dead | `abortAll` nulls `loading` whenever it aborts a load; the recheck has its own AbortController + watchdog and is aborted too; tests |
| 3 | P1 | a pointer adopted after the recheck resolved (its chunk landed later) never moved the own entry's window: the line said "newest 17:29" while the window ended at 14:59 | `_onSatChange` re-syncs the own window when `win.ref !== newestMs()`; test through `_satRecheck` |
| 4 | P1 | `_goto` called `_attribute()` before `_syncSat(idx)`: the " · Copernicus Marine" credit one landing behind | `_syncSat` before `_attribute` at the landing; `_syncSat` / `_clearSatWindow` re-attribute themselves; test |
| 5 | P2 | one failed pointer read left the box on "readings unavailable" for the session | `_satReload`: the landings and the 12-min recheck ask again once the cooldown passed; test |
| 6 | P2 | hs → tp with the box on: the dots stayed alone under "Loading model frame…" through the pointer re-read | `mount` detaches the satellite layer at once when the field is not wave height |
| 7 | P2 | a pan at 4x left a satellite buffer tile a landing behind for ~0.5 s | `move` draws the satellite layer's stale tiles in view like the model's |
| 8 | P2 | `recheck()` had no watchdog: a hung re-read latched `checking` | (as 2) |
| 9 | P2 | `_rebuild`: an Array copy + comparator sort of up to 600k indices per landing burst (130-210 ms) | a typed `Float64Array.sort` on packed keys (seconds x 2^20 + index), no comparator, no Array |
| 10 | P2 | the sample cap truncated the NEWEST readings when a pointer's chunks exceeded it in all | `validSatPointer` refuses a pointer whose chunks' `n` sum past MAX_SAT_SAMPLES |
| 11 | P2 | the swell hit radius (10 px) missed a long arrow's head and tail | (as 1: the shaft and head test) |
| 12 | P2 | after pressing "Swell" the focus came back on "Heights" | `_refocus` prefers `.ov-seg-btn.ov-seg-on` |
| 13 | P2 | the state note sat inside the box's `<label>` (an unstable accessible name) | the note beside the label with `aria-describedby`; test |
| N | nit | the box's frame window stayed drawn under "Loading satellite waves…"; `_armRetry` kept the first (longer) cooldown; the recheck ran in a hidden tab | `_mountObs('sat')` clears the frame window; `_armRetry` re-arms for an earlier cooldown; the recheck skips a hidden tab |
| N | nit | the frame credit counts readings in the window, not on screen | left: the plan's wording ("while readings are drawn") is met |

Mutants after the fixes: 20 of 22 caught; the two left are behaviour-equivalent (the stride at exactly zoom 7 is 1 either way; `_goto` sets `frameIndex` before the landing hook).
