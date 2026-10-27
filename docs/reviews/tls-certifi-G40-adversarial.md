# Section 57: the TLS trust list after eMudhra's compromise (G40)

Plan: `swirling-spinning-snowflake.md`, section 57. Branch `fix/tls-certifi` off `Live-Buoy-Update` @ a58e14a.
Render's advisory (2026-10-26): eMudhra's stolen intermediate keys (disclosed 2026-10-19); Python sites should require
certifi 2026.10.19 or later and redeploy; two test hosts, `dv-g3b-emudhra.we-have-a-problem.com` and
`dv-g3b-aaa-emudhra.we-have-a-problem.com`, must both be refused by the service's own HTTP client.

## Step 2: test-site verification (2026-10-27, Opus 5.5 at MAX)

Target: the site files of `fix/tls-certifi` (cff8f28: `requirements.txt` with `certifi>=2026.10.23`, `app.py` without
the extra CA bundle and with `/healthz` -> `tls`, `certs/` removed, `tests/test_swan_cert.py`), cherry-picked onto `test`
@ 895bee5; then an empty commit @ c556894 for a later rebuild. Scratch: `tls57/v2` (watch.py, capture.py, compare.py,
smoke.js).

### The site

- **First deploy** (test @ 895bee5, 17:17:56 UTC): 42 polls of `/robots.txt` and `/healthz` across the switch, 0 failed.
  The new instance's `/healthz` -> `tls`: `{"certifi": "2026.10.23", "bundle_is_certifi": true, "env_override": []}`.
- **Later rebuild** (test @ c556894, an empty commit, so Render built with its cache, 19:45:02 UTC): the new instance
  from 19:46:06, 44 polls 0 failed, the same `tls` on every poll of both instances. On the rebuilt instance every live
  feed (NDBC, CDIP, QLD, AODN, AusWaves, MI-IE, CEFAS, SMHI, RWS, CMEMS) and every wind feed (NDBC, METAR, CO-OPS, NWS,
  MADIS) published with no error; `/healthz` holds no error and no "ssl", "certificate" or "verify" anywhere; a tide
  window (1612340, harmonic, 1,585 points) and the SWAN table of 51201 (wind complete) answered.
- The Render build log was not read (the dashboard asks for a sign-in). `/healthz` reports the certifi the running
  process imported, which is what decides what it trusts.
- **Same answers as production**: 65 forecast answers (30 GFS compact, 6 classic, 5 Metric, 5 UTC, 12 SWAN, 3 lake,
  2 forecast points, 2 removed stations) on run 2026102712, captured 19:41-19:42 UTC from both sites: 65 identical. The
  12 SWAN tables (PacIOOS: the request that used the removed root) carry wind in every row.
- **Browser** (headless Chrome with its own profile; desktop 1280x800 and phone 375x812): 51201 GFS 385 rows, 51201 and
  51208 SWAN 175 rows, wind in every row, no error box, no sideways scroll; console 0 problems; 38 of 38 checks.
- **Test-branch suite**: 1,201 passed plus the five failures the test branch always has (its logo lines against the
  page golden x4, the numpy-import test).

### GitHub's runners (the temporary dry run, four runs)

A push-triggered workflow on the branch (no secrets, nothing published) ran `tools/tls_check.py` in the three Python
environments the jobs use, and curl the way coast-build uses it. Run 4 (b48113e) is the final state:

| Environment | Versions | Result |
|---|---|---|
| publishers (micromamba `frames` env) | python 3.11.16, openssl 3.6.5, certifi + ca-certificates 2026.10.23, botocore 1.43.118 | certifi, conda's OpenSSL default and botocore's file refuse both test hosts; real hosts verify (3 unreachable from the runner: reported as such) |
| coast-build's pip jobs | python 3.11.17, openssl 3.0.13, certifi 2026.10.23, botocore 1.43.119 (botocore uses certifi's file) | both test hosts refused, real hosts verify |
| the site's environment | python 3.11.9 + `requirements.txt`: certifi 2026.10.23, requests 2.34.2 | both refused; `_tls_health()` = `{"certifi": "2026.10.23", "bundle_is_certifi": true, "env_override": []}` |
| Node | `setup-node` "24" with `check-latest` | v24.22.1 |
| curl (Ubuntu 24.04) | `--cacert "$CA" --capath "$NOCAPATH"` | both test hosts exit 60 (refused); GitHub, raw.githubusercontent and GeoNames pass |

Findings fixed during the step (each on the branch, with pins in `tests/model_frames/test_tls_pins.py`):

- **V2-1: the runners' own list trusts a compromised chain.** On 2026-10-27 Ubuntu 24.04's `/etc/ssl/certs` still
  trusted `dv-g3b-emudhra` (curl exit 0). coast-build's curl downloads (GSHHG, Natural Earth, GeoNames) used it; the
  GeoNames file has no pinned digest. Fixed @ d44d5a5: every curl gets certifi's file.
- **V2-2: `--cacert` alone is not enough.** Ubuntu's curl keeps reading its built-in CA folder `/etc/ssl/certs` when only
  `--cacert` is given: `dv-g3b-emudhra` exit 0 again. Fixed @ b48113e: `--capath` an empty directory; run 4 shows exit 60
  for both test hosts with the fix and exit 0 for `dv-g3b-emudhra` with `--cacert` alone or the runner's default.
- **V2-3: a network failure is not a certificate result.** A runner that could not reach a real host (PacIOOS,
  CoastWatch at times) failed the whole check and skipped the steps after it. Fixed @ d4d46e7 / b48113e: a real host is
  asked twice and then reported as unreachable; a test host that cannot be reached still fails (nothing was proved); the
  curl step runs even when the Python check failed.

The temporary workflow is removed @ ef85bfb, with a test that it stays gone; model-frames-tests now also runs the pin
tests when `tools/tls_check.py` or `coast-build.yml` changes.

### Corrections to the step-1 notes

- Commit 5d78043 holds only the PEM deletion: its `git add` named the already deleted `certs` path, failed with its error
  hidden, and the commit went ahead with what was staged. Its message describes more. The real step-1 change is
  cff8f28 (checked with `git show --stat`).
- "GitHub started no run" was wrong: 5d78043 and 36fd9dd changed no workflow, so no workflow had a reason to run;
  cff8f28 ran normally.

### Notes

- Production looks for a new NOAA run only when someone asks it for a forecast (section 53: the run is cached 30
  minutes and re-checked on demand). With no visitor for 2.5 hours it stayed on the 06Z run until one request. By
  design; noted because the comparison waited on it.
- The laptop (owner's go-ahead, step 5) still has certifi 2025.04.26, which trusted `dv-g3b-emudhra` in the local check.
