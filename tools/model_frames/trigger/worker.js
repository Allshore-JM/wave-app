// Cloudflare Worker: dispatches the model-frames workflow on a cron schedule. GitHub drops or
// delays most of its own scheduled ticks (see .github/workflows/model-frames.yml), so this Worker
// asks the GitHub API to start the workflow every ten minutes; the job itself decides in ~20 s
// whether a new NOAA cycle is complete and short-circuits otherwise. GitHub's schedule stays on
// as a fallback -- a duplicate run just short-circuits behind the concurrency group.
//
// Cron-only: no fetch handler and no public URL (workers_dev = false in wrangler.jsonc).
// Secret GITHUB_TOKEN: a fine-grained token for the repository with "Actions: read and write",
// entered in the Cloudflare dashboard, never in this repository. Plain vars (wrangler.jsonc):
// GH_REPO, GH_WORKFLOW, GH_REF.

export async function dispatch(env, fetchImpl = fetch) {
  const url = `https://api.github.com/repos/${env.GH_REPO}/actions/workflows/${env.GH_WORKFLOW}/dispatches`;
  const res = await fetchImpl(url, {
    method: "POST",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "allshoresurf-model-frames-trigger",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref: env.GH_REF }),
  });
  if (res.status !== 204) {
    const body = (await res.text()).slice(0, 300);
    throw new Error(`dispatch ${env.GH_WORKFLOW}@${env.GH_REF}: HTTP ${res.status} ${body}`);
  }
  return res.status;
}

export default {
  async scheduled(controller, env) {
    if (!env.GITHUB_TOKEN) {
      throw new Error("GITHUB_TOKEN secret is not set (Worker settings > Variables and Secrets)");
    }
    await dispatch(env);
    console.log(`dispatched ${env.GH_WORKFLOW}@${env.GH_REF} (cron ${controller.cron})`);
  },
};
