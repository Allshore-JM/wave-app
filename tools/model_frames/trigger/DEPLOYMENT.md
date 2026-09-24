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
| `GH_REPO`, `GH_WORKFLOW`, `GH_REF` | plain vars from `wrangler.jsonc` | `Allshore-JM/wave-app`, `model-frames.yml`, `Live-Buoy-Update` |
| `GITHUB_TOKEN` | **secret, entered in the dashboard** | GitHub fine-grained personal access token: repository access = only `Allshore-JM/wave-app`, repository permission Actions = Read and write (Metadata read is implied), expiry per policy. Survives deploys. Rotate here. |

Cron: `5,15,25,35,45,55 * * * *` UTC (Settings -> Trigger events after a successful deploy). Each tick
starts one run of `model-frames.yml` on `Live-Buoy-Update`; the job short-circuits in ~20 s when the live run is
current. Runs it starts appear in GitHub Actions with the event `workflow_dispatch` and the token's owner as actor.

Checks: a build must show the Installing / Building / Deploying stages (a missing Building stage means the build
command is empty); a failed cron tick is visible under Metrics (errors) and Observability (logs) -- the common cause
is a missing or expired `GITHUB_TOKEN` (HTTP 401) or a token without the Actions permission (HTTP 403/404).
To stop the trigger: Settings -> Trigger events -> remove the cron, or delete the Worker; GitHub's own schedule in
the workflow keeps publishing (late) on its own.
