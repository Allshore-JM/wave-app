# G36: GFS bulletins and station winds without NOMADS (plan section 53)

## G36a: the GFS winds job (step 2), 2026-10-25

One fresh-context reviewer at HIGH on `feat/gfs-bulletins` @ b839a80: `tools/model_frames/gfs_winds.py`, its workflow, the
trigger / keepalive / tests-path wiring, the tests and the README section. The site side (step 1, 84be491) was out of
scope.

**0 P0, 0 P1, 2 P2, 7 P3.**

### What held up (the reviewer's own evidence)
- **C1 values:** the whole archive of run 2026102512, 918 stations x 385 hours = 353,430 cells, matched the site's
  `app._parse_spec_wind_text` and an independent parser of the reviewer's own, with 0 differences. Its winds.bin sha256
  (`db1ef486...`) equals the author's local and runner builds. 12 stations matched their NOMADS `.spec`, 0 differences.
- **C2-C6, C8, C9:**
  - encoding round trip, steps, publish order (the pointer last), prune rules, hostile archives, discovery on the
    scheduled path, workflow pins and secrets;
  - memory: 53 MB peak working set on a 600-member build.
- **C7 only in part:** a stall is cut off at STALL_S but was filed as a budget exit (P2-1).

### Findings and outcomes

| # | Finding | Outcome |
|---|---|---|
| P2-1 | On Python 3.10+ `socket.timeout` IS `TimeoutError`, so a stalled download was taken for the build budget: exit 3 and no failure record (a local stalling server: rc 3, no record). | FIXED: the budget has its own `BudgetExceeded`; a stall is a recorded failure (exit 1). Test with a stream that times out like a socket. |
| P2-2 | While a newer run could not be built (budget, stall, NotReady, 6 failed attempts) every tick exited 3 without the stale check: a 30-h-old live run gave rc 3 and no alarm, every workflow run green. | FIXED: `_not_now` runs the stale check on every such exit; the budget exit is also counted under `notready/`. Tests for budget / notready / failed x6. |
| P3-1 | `--run` skipped the never-regress rule (the pointer moved 12Z -> 06Z) and rewrote a published run's immutable index.json. | FIXED: `--run` of an older run is refused (exit 2); a run with an index is never rewritten (exit 0, nothing written). |
| P3-2 | The gzip CRC32 / ISIZE were never checked (tar stream mode stops at the end-of-archive block): a flipped CRC or a cut trailer built fine. | FIXED: the gzip stream is read to its end after the tar (inside the budget). |
| P3-3 | Nothing tied a member to the run: the previous cycle's archive under this run's name built, 6 h shifted. | FIXED: at least 90 % of members must start at the run's hour (one member missing its first hour still builds; tested). |
| P3-4 | `FLOAT_RE` was quadratic on a long digit run in the unbounded point-line group (40 KB: 1.9 s; 1 MB: ~20 min by extrapolation), with no budget check inside a member. Hostile input only. | FIXED: the point line is bounded (`POINT_LINE_MAX` 400 B after a quoted name of at most 64 B; real lines ~62 B); a longer line is no point line. |
| P3-5 | One transient 503 / reset on the GET is a failure with a 1-h wait, while a stall used to be retried at once. | ACCEPTED: the frames jobs' policy (NOMADS's `.spec` covers the hour). The two now agree: both are failures. |
| P3-6 | Test gaps: 12 of 34 mutants survived (prune's pointer protection, failed/notready records, the budget exit code, the already-live stale alarm, step 385, the finiteness guard, `--run` without the size check). The site-parity test never runs in CI (no Flask). No test pinned the removal of the dry-run workflow. | FIXED: tests for each; the reviewer's 34 mutants re-run: 3 survive, all equivalent (M02: a minus sign never changes which float is 4th / 5th; M17: non-run folders sort after every run and are never deleted; M33: a short member read is tarfile's ReadError first). The parity test runs in the site suite (it has Flask); CI checks the captured real member's values. The dry-run workflow is removed, with a test. |
| P3-7 | Nits: `head_size` slept after its last try; failed/notready records never pruned; the Worker and wrangler comments did not name the fifth workflow; `--out` silently ignored in live mode. | FIXED: no sleep after the last try; records of runs older than the oldest kept run are pruned; comments; `--out` without a dry run is refused (exit 2). |

The fix round's own changes: 15 mutants, 15 killed. Job suite 333 passed; the trigger Worker's Node tests pass.
