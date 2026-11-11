# Deployment: allshore-model-frames-trigger (Cloudflare Workers Builds)

The Worker is deployed by Cloudflare's Git integration (Workers Builds), not by hand. Dashboard:
Workers & Pages -> allshore-model-frames-trigger -> Settings -> Builds.

| Setting | Value |
|---|---|
| Git repository | Allshore-JM/wave-app |
| Root directory | `tools/model_frames/trigger` |
| Build command | `npm run build` (syntax check + the fetch-stubbed unit tests; must not be empty) |
| Deploy command | `npx wrangler deploy` (reads `wrangler.jsonc`: name, cron, vars, no workers.dev URL) |
| Production branch | `Live-Buoy-Update` (the production branch; the job and this trigger live there together) |
| Build watch paths | include `tools/model_frames/trigger/*` (pushes elsewhere in the repo do not build) |
| Preview builds | off |
| API token | `allshore-model-frames-trigger build token`, created for this Worker alone (Settings -> Builds -> API token -> Create new token); no other project depends on it and it depends on no other project's token |

Runtime configuration (Settings -> Variables and secrets, Production):

| Name | Kind | Value |
|---|---|---|
| `GH_REPO`, `GH_WORKFLOW`, `GH_REF`, `GH_PERIOD`, `GH_MINUTE`, `GH_MAX_ACTIVE` | plain vars from `wrangler.jsonc` | `Allshore-JM/wave-app`, the list in `wrangler.jsonc` (a comma-separated list: every workflow in it is dispatched on every tick; the satellite jobs `model-alt.yml` and `model-sarswell.yml` are in it since plan section 81, the radar job `model-radar.yml` since section 85; the daily `model-altgrid.yml` of plan section 82 is not: its own cron suffices), `Live-Buoy-Update`; `GH_HOURLY` = `model-sst.yml`, dispatched at minute 5 of each hour only (GitHub's own hourly cron for it fired only every 6-9 h); `GH_PERIOD` = `10` and `GH_MINUTE` = `model-radar.yml=5`: each workflow at its own minute of the ten (radar at :05, :15 ...); `GH_MAX_ACTIVE` = `8`: a workflow waits for its next minute while 8 or more of the repository's runs are running or queued (plan section 94) |
| `GITHUB_TOKEN` | **secret, entered in the dashboard** | GitHub fine-grained personal access token: repository access = only `Allshore-JM/wave-app`, repository permission Actions = Read and write (Metadata read is implied), expiry per policy. Survives deploys. Rotate here. |

Cron: `* * * * *` UTC (Settings -> Trigger events after a successful deploy; plan section 94). Each minute starts the
one workflow whose minute it is (`GH_PERIOD` 10: every workflow once per 10 minutes; `GH_MINUTE` pins the radar job to
:05, :15 ...; the others take minutes 0-4 and 6-9 in `GH_WORKFLOW`'s order; `model-sst.yml` also at minute 5 of the
hour) on `Live-Buoy-Update`, instead of all ten in the same second: GitHub's free plan runs 20 jobs at once across the
whole account. Before it dispatches, the tick counts the repository's runs that are running or queued (two Actions API
calls with `per_page=1`, reading only `total_count`); at `GH_MAX_ACTIVE` (8) or more the workflow is held to its next
minute (10 min later) and the log says `held ...`; a failed count never stops a dispatch (`count failed ...;
dispatched anyway`). Each job short-circuits in ~20 s when its live run is current, and a workflow that cannot be
dispatched ends the tick as an error naming it. Runs it starts appear in GitHub Actions with the event
`workflow_dispatch` and the token's owner as actor. The token's "Actions: read and write" covers the run count.
Cost: 1,440 invocations a day (Workers Free allows 100,000) and up to 3 GitHub API calls a minute (the token's limit is
5,000 an hour). To go back to the old pattern (every workflow on every tick): remove `GH_PERIOD` and set the cron
back to `5,15,25,35,45,55 * * * *`.

Checks: a build must show the Installing / Building / Deploying stages (a missing Building stage means the build
command is empty); a failed cron tick is visible under Metrics (errors) and Observability (logs) -- the common cause
is a missing or expired `GITHUB_TOKEN` (HTTP 401) or a token without the Actions permission (HTTP 403/404).
To stop the trigger: Settings -> Trigger events -> remove the cron, or delete the Worker; GitHub's own schedule in
the workflow keeps publishing (late) on its own.
