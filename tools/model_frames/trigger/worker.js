// Cloudflare Worker: dispatches the model workflows on a cron schedule. GitHub drops or
// delays most of its own scheduled ticks (see .github/workflows/model-frames.yml), so this Worker
// asks the GitHub API to start each workflow every ten minutes; the job itself decides in ~20 s
// whether a new NOAA cycle is complete and short-circuits otherwise. GitHub's schedule stays on
// as a fallback -- a duplicate run just short-circuits behind the concurrency group.
// GH_WORKFLOW is one workflow file or a comma-separated list (the overlay frames and the forecast
// points); every one is dispatched on every tick, and one that fails never holds back the others.
//
// Cron-only: no fetch handler and no public URL (workers_dev = false in wrangler.jsonc).
// Secret GITHUB_TOKEN: a fine-grained token for the repository with "Actions: read and write",
// entered in the Cloudflare dashboard, never in this repository. Plain vars (wrangler.jsonc):
// GH_REPO, GH_WORKFLOW, GH_REF.

export function workflows(env) {
  return String(env.GH_WORKFLOW || "").split(",").map((s) => s.trim()).filter(Boolean);
}

export async function dispatch(env, fetchImpl = fetch, workflow = workflows(env)[0]) {
  const url = `https://api.github.com/repos/${env.GH_REPO}/actions/workflows/${workflow}/dispatches`;
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
    throw new Error(`dispatch ${workflow}@${env.GH_REF}: HTTP ${res.status} ${body}`);
  }
  return res.status;
}

// Every workflow is tried; the failures (if any) are thrown together afterwards.
export async function dispatchAll(env, fetchImpl = fetch) {
  const list = workflows(env);
  if (!list.length) throw new Error("GH_WORKFLOW names no workflow");
  const done = [], failed = [];
  for (const workflow of list) {
    try {
      await dispatch(env, fetchImpl, workflow);
      done.push(workflow);
    } catch (e) {
      failed.push(e && e.message ? e.message : String(e));
    }
  }
  if (failed.length) throw new Error(failed.join(" | "));
  return done;
}

export default {
  async scheduled(controller, env) {
    if (!env.GITHUB_TOKEN) {
      throw new Error("GITHUB_TOKEN secret is not set (Worker settings > Variables and Secrets)");
    }
    const done = await dispatchAll(env);
    console.log(`dispatched ${done.join(", ")}@${env.GH_REF} (cron ${controller.cron})`);
  },
};
