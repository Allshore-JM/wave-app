// Cloudflare Worker: dispatches the model workflows on a cron schedule. GitHub drops or
// delays most of its own scheduled ticks (see .github/workflows/model-frames.yml), so this Worker
// asks the GitHub API to start each workflow every ten minutes; the job itself decides in ~20 s
// whether a new model cycle is complete and short-circuits otherwise. GitHub's schedule stays on
// as a fallback -- a duplicate run just short-circuits behind the concurrency group.
// GH_WORKFLOW is one workflow file or a comma-separated list (the overlay frames, the forecast points, the ECMWF frames,
// the Great Lakes frames, the GFS station winds and the NWPS tables); every one is dispatched on every tick, and one that fails never
// holds back the others.
//
// GH_HOURLY (optional, the same form): workflows dispatched on the FIRST tick of each hour only (minute < 10), for
// products that change once a day and need no 10-minute polling (the SST day: GitHub fired its own hourly cron only
// every 6-9 h, 2026-11-05). Owner: hourly is enough.
//
// Cron-only: no fetch handler and no public URL (workers_dev = false in wrangler.jsonc).
// Secret GITHUB_TOKEN: a fine-grained token for the repository with "Actions: read and write",
// entered in the Cloudflare dashboard, never in this repository. Plain vars (wrangler.jsonc):
// GH_REPO, GH_WORKFLOW, GH_REF.

function list(value) {
  return String(value || "").split(",").map((s) => s.trim()).filter(Boolean);
}

export function workflows(env) {
  return list(env.GH_WORKFLOW);
}

// The workflows of one tick: GH_WORKFLOW always, GH_HOURLY on the hour's first tick (UTC minute < 10).
export function tickWorkflows(env, when = new Date()) {
  const out = workflows(env);
  if (new Date(when).getUTCMinutes() < 10) {
    for (const w of list(env.GH_HOURLY)) if (!out.includes(w)) out.push(w);
  }
  return out;
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
export async function dispatchAll(env, fetchImpl = fetch, names = workflows(env)) {
  if (!names.length) throw new Error("GH_WORKFLOW names no workflow");
  const done = [], failed = [];
  for (const workflow of names) {
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
    const when = controller.scheduledTime ? new Date(controller.scheduledTime) : new Date();
    const done = await dispatchAll(env, fetch, tickWorkflows(env, when));
    console.log(`dispatched ${done.join(", ")}@${env.GH_REF} (cron ${controller.cron})`);
  },
};
