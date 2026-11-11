// Cloudflare Worker: dispatches the model workflows on a cron schedule. GitHub drops or
// delays most of its own scheduled ticks (see .github/workflows/model-frames.yml), so this Worker
// asks the GitHub API to start each workflow every ten minutes; the job itself decides in ~20 s
// whether a new model cycle is complete and short-circuits otherwise. GitHub's schedule stays on
// as a fallback -- a duplicate run just short-circuits behind the concurrency group.
// GH_WORKFLOW is one workflow file or a comma-separated list (the overlay frames, the forecast points, the ECMWF frames,
// the Great Lakes frames, the GFS station winds, the NWPS tables ...); one that fails never holds back the others.
//
// Spread over the period (plan section 94): with GH_PERIOD set (minutes, e.g. "10") and a cron that fires every
// minute, each workflow has its own minute of the period and is dispatched once per period at that minute, instead
// of all of them in the same second (GitHub's free plan runs 20 jobs at once across the whole account; ten runs
// started together left other repositories' jobs waiting). GH_MINUTE pins a workflow to a minute ("name=5", a
// comma-separated list): the radar job keeps its :x5 minute, which its slot timing assumes. The other workflows
// take the remaining minutes in GH_WORKFLOW's order. Without GH_PERIOD every workflow is dispatched on every tick.
//
// Cap (plan section 94): with GH_MAX_ACTIVE set, a tick first counts the repository's runs that hold or wait for a
// runner (status in_progress + queued; a run waiting on its concurrency group is "pending" and holds nothing) and
// dispatches only while the count is below the cap. A workflow held back this way runs at its next minute (one period
// later). A failed count never stops a dispatch: the cap is a courtesy to the account's other repositories.
//
// GH_HOURLY (optional, the same form): workflows dispatched once an hour only, for products that change once a day
// and need no 10-minute polling (the SST day: GitHub fired its own hourly cron only every 6-9 h, 2026-11-05). Owner:
// hourly is enough. Without GH_PERIOD: on the hour's first tick (UTC minute < 10); with it: at minute HOURLY_MINUTE.
//
// Cron-only: no fetch handler and no public URL (workers_dev = false in wrangler.jsonc).
// Secret GITHUB_TOKEN: a fine-grained token for the repository with "Actions: read and write",
// entered in the Cloudflare dashboard, never in this repository. Plain vars (wrangler.jsonc):
// GH_REPO, GH_WORKFLOW, GH_REF, GH_HOURLY, GH_PERIOD, GH_MINUTE, GH_MAX_ACTIVE.

export const HOURLY_MINUTE = 5;          // the hourly workflows' minute when GH_PERIOD is set (= the old :05 tick)

const HEADERS = (env) => ({
  Accept: "application/vnd.github+json",
  Authorization: `Bearer ${env.GITHUB_TOKEN}`,
  "X-GitHub-Api-Version": "2022-11-28",
  "User-Agent": "allshoresurf-model-frames-trigger",
});

function list(value) {
  return String(value || "").split(",").map((s) => s.trim()).filter(Boolean);
}

// A whole number from a plain var, or 0 when unset / not a positive whole number.
function whole(value) {
  const s = String(value == null ? "" : value).trim();
  return /^[0-9]+$/.test(s) ? Number(s) : 0;
}

export function workflows(env) {
  return list(env.GH_WORKFLOW);
}

export function periodOf(env) {
  const p = whole(env.GH_PERIOD);
  return p >= 1 && p <= 60 ? p : 0;
}

export function maxActive(env) {
  return whole(env.GH_MAX_ACTIVE);
}

// Each GH_WORKFLOW entry's minute of the period: GH_MINUTE's pins first, then the free minutes in list order
// (wrapping when there are more workflows than minutes).
export function minutesOf(env, period = periodOf(env)) {
  const names = workflows(env);
  const out = new Map();
  if (!period) return out;
  for (const pin of list(env.GH_MINUTE)) {
    const i = pin.lastIndexOf("=");
    const name = i > 0 ? pin.slice(0, i).trim() : "";
    const m = i > 0 ? whole(pin.slice(i + 1)) : -1;
    if (!names.includes(name) || !/^[0-9]+$/.test(pin.slice(i + 1).trim()) || m >= period) {
      throw new Error(`GH_MINUTE entry "${pin}": needs a GH_WORKFLOW name and a minute 0-${period - 1}`);
    }
    out.set(name, m);
  }
  const taken = new Set(out.values());
  const free = [];
  for (let m = 0; m < period; m++) if (!taken.has(m)) free.push(m);
  let k = 0;
  for (const name of names) {
    if (out.has(name)) continue;
    out.set(name, free.length ? free[k % free.length] : k % period);
    k += 1;
  }
  return out;
}

// The workflows due at this tick.
export function tickWorkflows(env, when = new Date()) {
  const minute = new Date(when).getUTCMinutes();
  const period = periodOf(env);
  let out;
  let hourly;
  if (period) {
    const at = minutesOf(env, period);
    out = workflows(env).filter((w) => at.get(w) === minute % period);
    hourly = minute === HOURLY_MINUTE;
  } else {
    out = workflows(env);
    hourly = minute < 10;
  }
  if (hourly) {
    for (const w of list(env.GH_HOURLY)) if (!out.includes(w)) out.push(w);
  }
  return out;
}

// The repository's runs that hold or wait for a runner: in_progress + queued (each asked with per_page=1: only
// total_count is read).
export async function activeRuns(env, fetchImpl = fetch) {
  let n = 0;
  for (const status of ["in_progress", "queued"]) {
    const url = `https://api.github.com/repos/${env.GH_REPO}/actions/runs?status=${status}&per_page=1&exclude_pull_requests=true`;
    const res = await fetchImpl(url, { method: "GET", headers: HEADERS(env) });
    if (res.status !== 200) {
      throw new Error(`count ${status}: HTTP ${res.status}`);
    }
    let count;
    try {
      count = JSON.parse(await res.text()).total_count;
    } catch (e) {
      throw new Error(`count ${status}: unreadable answer`);
    }
    if (!Number.isInteger(count) || count < 0) {
      throw new Error(`count ${status}: no total_count`);
    }
    n += count;
  }
  return n;
}

export async function dispatch(env, fetchImpl = fetch, workflow = workflows(env)[0]) {
  const url = `https://api.github.com/repos/${env.GH_REPO}/actions/workflows/${workflow}/dispatches`;
  const res = await fetchImpl(url, {
    method: "POST",
    headers: { ...HEADERS(env), "Content-Type": "application/json" },
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

// One tick: the due workflows, held to the cap. Returns {due, sent, held, active, countError}.
export async function tick(env, fetchImpl = fetch, when = new Date()) {
  if (!workflows(env).length) throw new Error("GH_WORKFLOW names no workflow");
  const due = tickWorkflows(env, when);
  const out = { due, sent: [], held: [], active: null, countError: null };
  if (!due.length) return out;
  let send = due;
  const cap = maxActive(env);
  if (cap) {
    try {
      out.active = await activeRuns(env, fetchImpl);
    } catch (e) {
      out.countError = e && e.message ? e.message : String(e);
    }
    if (out.active !== null) {
      const room = Math.max(0, cap - out.active);
      send = due.slice(0, room);
      out.held = due.slice(room);
    }
  }
  if (send.length) out.sent = await dispatchAll(env, fetchImpl, send);
  return out;
}

export default {
  async scheduled(controller, env) {
    if (!env.GITHUB_TOKEN) {
      throw new Error("GITHUB_TOKEN secret is not set (Worker settings > Variables and Secrets)");
    }
    const when = controller.scheduledTime ? new Date(controller.scheduledTime) : new Date();
    const r = await tick(env, fetch, when);
    const parts = [];
    if (r.sent.length) parts.push(`dispatched ${r.sent.join(", ")}@${env.GH_REF}`);
    if (r.held.length) parts.push(`held ${r.held.join(", ")}: ${r.active} runs active (cap ${maxActive(env)})`);
    if (r.countError) parts.push(`count failed (${r.countError}); dispatched anyway`);
    if (!r.due.length) parts.push("nothing due");
    console.log(`${parts.join("; ")} (cron ${controller.cron})`);
  },
};
