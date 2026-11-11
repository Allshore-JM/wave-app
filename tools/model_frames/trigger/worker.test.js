// node --test worker.test.js  (no network: fetch is stubbed)
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import worker, { activeRuns, dispatch, dispatchAll, HOURLY_MINUTE, maxActive, minutesOf, periodOf, tick, tickWorkflows, workflows } from "./worker.js";

const ENV = { GH_REPO: "Allshore-JM/wave-app", GH_WORKFLOW: "model-frames.yml", GH_REF: "Live-Buoy-Update", GITHUB_TOKEN: "t0k3n" };
const BOTH = { ...ENV, GH_WORKFLOW: "model-frames.yml,model-points.yml" };

function fakeFetch(status, body = "") {
  const calls = [];
  const fn = async (url, init) => { calls.push({ url, init }); return { status, text: async () => body }; };
  fn.calls = calls;
  return fn;
}

test("dispatch posts the workflow_dispatch request GitHub expects", async () => {
  const f = fakeFetch(204);
  assert.equal(await dispatch(ENV, f), 204);
  assert.equal(f.calls.length, 1);
  const { url, init } = f.calls[0];
  assert.equal(url, "https://api.github.com/repos/Allshore-JM/wave-app/actions/workflows/model-frames.yml/dispatches");
  assert.equal(init.method, "POST");
  assert.equal(init.headers.Authorization, "Bearer t0k3n");
  assert.equal(init.headers.Accept, "application/vnd.github+json");
  assert.equal(init.headers["X-GitHub-Api-Version"], "2022-11-28");
  assert.ok(init.headers["User-Agent"]);
  assert.deepEqual(JSON.parse(init.body), { ref: "Live-Buoy-Update" });
});

test("a non-204 answer is an error that names the status and the body", async () => {
  const f = fakeFetch(401, '{"message":"Bad credentials"}');
  await assert.rejects(dispatch(ENV, f), /HTTP 401 .*Bad credentials/);
  const g = fakeFetch(422, "x".repeat(1000));
  await assert.rejects(dispatch(ENV, g), (e) => e.message.length < 400);
});

test("scheduled refuses to run without the secret and never calls the API", async () => {
  const calls = [];
  const saved = globalThis.fetch;
  globalThis.fetch = async (...a) => { calls.push(a); return { status: 204, text: async () => "" }; };
  try {
    await assert.rejects(worker.scheduled({ cron: "5,15 * * * *" }, { ...ENV, GITHUB_TOKEN: "" }), /GITHUB_TOKEN/);
    assert.equal(calls.length, 0);
    await worker.scheduled({ cron: "5,15 * * * *" }, ENV);
    assert.equal(calls.length, 1);
    await worker.scheduled({ cron: "5,15 * * * *" }, BOTH);
    assert.deepEqual(calls.slice(1).map((c) => c[0].split("/workflows/")[1]), ["model-frames.yml/dispatches", "model-points.yml/dispatches"]);
  } finally {
    globalThis.fetch = saved;
  }
});

test("a list of workflows: each one is dispatched, in order", async () => {
  assert.deepEqual(workflows(BOTH), ["model-frames.yml", "model-points.yml"]);
  assert.deepEqual(workflows({ GH_WORKFLOW: " a.yml , ,b.yml " }), ["a.yml", "b.yml"]);
  const f = fakeFetch(204);
  assert.deepEqual(await dispatchAll(BOTH, f), ["model-frames.yml", "model-points.yml"]);
  assert.deepEqual(f.calls.map((c) => c.url), [
    "https://api.github.com/repos/Allshore-JM/wave-app/actions/workflows/model-frames.yml/dispatches",
    "https://api.github.com/repos/Allshore-JM/wave-app/actions/workflows/model-points.yml/dispatches",
  ]);
  assert.ok(f.calls.every((c) => JSON.parse(c.init.body).ref === "Live-Buoy-Update"));
  await assert.rejects(dispatchAll({ ...ENV, GH_WORKFLOW: " , " }, f), /names no workflow/);
});

test("one workflow failing never holds back the others, and the tick still reports the failure", async () => {
  const calls = [];
  const f = async (url) => {
    calls.push(url);
    return url.includes("model-frames.yml") ? { status: 404, text: async () => "Not Found" } : { status: 204, text: async () => "" };
  };
  await assert.rejects(dispatchAll(BOTH, f), /model-frames[.]yml@Live-Buoy-Update: HTTP 404/);
  assert.equal(calls.length, 2);                                   // the points workflow was still dispatched
  const g = async (url) => ({ status: url.includes("model-points.yml") ? 422 : 204, text: async () => "no" });
  await assert.rejects(dispatchAll(BOTH, g), /model-points[.]yml@Live-Buoy-Update: HTTP 422/);
  const h = async () => ({ status: 500, text: async () => "x" });
  await assert.rejects(dispatchAll(BOTH, h), /model-frames[.]yml.* [|] .*model-points[.]yml/);
});

test("a fetch that THROWS for one workflow (a network error) never holds back the others", async () => {
  const calls = [];
  const f = async (url) => {
    calls.push(url);
    if (url.includes("model-frames.yml")) throw new TypeError("fetch failed");
    return { status: 204, text: async () => "" };
  };
  await assert.rejects(dispatchAll(BOTH, f), /fetch failed/);
  assert.equal(calls.length, 2);
  assert.ok(calls[1].includes("model-points.yml"));
  const g = async (url) => {
    calls.push(url);
    if (url.includes("model-points.yml")) throw new Error("socket hang up");
    return { status: 204, text: async () => "" };
  };
  await assert.rejects(dispatchAll(BOTH, g), /socket hang up/);
  assert.ok(calls[2].includes("model-frames.yml"));                 // the frames were dispatched first, and were
});

test("GH_HOURLY is added on the hour's first tick only, once, after the 10-minute list", async () => {
  const env = { ...BOTH, GH_HOURLY: "model-sst.yml, model-points.yml" };
  const at = (m) => new Date(Date.UTC(2026, 10, 5, 17, m));
  assert.deepEqual(tickWorkflows(env, at(5)), ["model-frames.yml", "model-points.yml", "model-sst.yml"]);
  assert.deepEqual(tickWorkflows(env, at(9)), ["model-frames.yml", "model-points.yml", "model-sst.yml"]);
  for (const m of [10, 15, 25, 35, 45, 55]) assert.deepEqual(tickWorkflows(env, at(m)), ["model-frames.yml", "model-points.yml"]);
  assert.deepEqual(tickWorkflows(BOTH, at(5)), ["model-frames.yml", "model-points.yml"]);   // none set: as before
  const calls = [];
  const saved = globalThis.fetch;
  globalThis.fetch = async (url) => { calls.push(url.split("/workflows/")[1]); return { status: 204, text: async () => "" }; };
  try {
    await worker.scheduled({ cron: "5,15 * * * *", scheduledTime: at(5).getTime() }, env);
    await worker.scheduled({ cron: "5,15 * * * *", scheduledTime: at(15).getTime() }, env);
  } finally {
    globalThis.fetch = saved;
  }
  assert.deepEqual(calls, ["model-frames.yml/dispatches", "model-points.yml/dispatches", "model-sst.yml/dispatches",
                           "model-frames.yml/dispatches", "model-points.yml/dispatches"]);
});

// ---------------------------------------------------------------- plan section 94: spread over the period + the cap
const TEN = "model-frames.yml,model-points.yml,model-ecmwf.yml,model-glwu.yml,model-gfswinds.yml,model-gfsspec.yml,model-nwps.yml,model-alt.yml,model-sarswell.yml,model-radar.yml";
const SPREAD = { ...ENV, GH_WORKFLOW: TEN, GH_PERIOD: "10", GH_MINUTE: "model-radar.yml=5", GH_HOURLY: "model-sst.yml" };
const at = (h, m) => new Date(Date.UTC(2026, 10, 11, h, m));

// A GitHub API stand-in: the run counts answered from inProgress / queued, dispatches recorded.
function api({ inProgress = 0, queued = 0, countStatus = 200, countBody = null, dispatchStatus = 204 } = {}) {
  const calls = [];
  const fn = async (url, init) => {
    calls.push({ url, init });
    if (url.includes("/actions/runs?")) {
      const n = url.includes("status=in_progress") ? inProgress : queued;
      const body = countBody !== null ? countBody : JSON.stringify({ total_count: n, workflow_runs: [] });
      return { status: countStatus, text: async () => body };
    }
    return { status: dispatchStatus, text: async () => "" };
  };
  fn.calls = calls;
  fn.sent = () => calls.filter((c) => c.url.endsWith("/dispatches")).map((c) => c.url.split("/workflows/")[1].split("/")[0]);
  fn.counts = () => calls.filter((c) => c.url.includes("/actions/runs?")).length;
  return fn;
}

test("the spread: radar keeps its :x5 minute, the others take the free minutes in list order", () => {
  assert.equal(periodOf(SPREAD), 10);
  const m = minutesOf(SPREAD);
  assert.deepEqual([...m.entries()], [
    ["model-radar.yml", 5], ["model-frames.yml", 0], ["model-points.yml", 1], ["model-ecmwf.yml", 2], ["model-glwu.yml", 3],
    ["model-gfswinds.yml", 4], ["model-gfsspec.yml", 6], ["model-nwps.yml", 7], ["model-alt.yml", 8], ["model-sarswell.yml", 9],
  ]);
  // more workflows than minutes: they wrap onto the free minutes again
  const many = minutesOf({ GH_WORKFLOW: "a,b,c,d,e", GH_PERIOD: "3", GH_MINUTE: "c=1" });
  assert.deepEqual(Object.fromEntries(many), { c: 1, a: 0, b: 2, d: 0, e: 2 });
  // every minute pinned: the rest wrap over the whole period
  const full = minutesOf({ GH_WORKFLOW: "a,b,c", GH_PERIOD: "2", GH_MINUTE: "a=0,b=1" });
  assert.deepEqual(Object.fromEntries(full), { a: 0, b: 1, c: 0 });
  // no period: no minutes (every workflow on every tick)
  assert.equal(minutesOf({ GH_WORKFLOW: TEN }).size, 0);
  for (const bad of ["model-radar.yml=10", "model-radar.yml=x", "nope.yml=3", "=3", "model-radar.yml", "model-radar.yml=-1", "model-radar.yml= "]) {
    assert.throws(() => minutesOf({ ...SPREAD, GH_MINUTE: bad }), /GH_MINUTE/, bad);
  }
  for (const [v, want] of [["10", 10], [" 10 ", 10], ["0", 0], ["", 0], [undefined, 0], ["61", 0], ["60", 60], ["x", 0], ["5.5", 0], ["-3", 0]]) {
    assert.equal(periodOf({ GH_PERIOD: v }), want, String(v));
  }
});

test("the spread: one workflow a minute, each once per period, the hourly one at minute 5 only", () => {
  const minutes = minutesOf(SPREAD);
  for (let m = 0; m < 60; m++) {
    const due = tickWorkflows(SPREAD, at(17, m));
    const want = [...minutes].filter(([, mm]) => mm === m % 10).map(([w]) => w);
    if (m === HOURLY_MINUTE) want.push("model-sst.yml");
    assert.deepEqual(due, want, `minute ${m}`);
  }
  assert.deepEqual(tickWorkflows(SPREAD, at(17, 5)), ["model-radar.yml", "model-sst.yml"]);
  assert.deepEqual(tickWorkflows(SPREAD, at(17, 15)), ["model-radar.yml"]);
  assert.deepEqual(tickWorkflows(SPREAD, at(17, 0)), ["model-frames.yml"]);
  assert.deepEqual(tickWorkflows(SPREAD, at(17, 59)), ["model-sarswell.yml"]);
  // a whole day: every workflow 144 times, the SST one 24, radar only at minutes ending in 5, at most 2 in a minute
  const count = new Map();
  let most = 0;
  for (let h = 0; h < 24; h++) {
    for (let m = 0; m < 60; m++) {
      const due = tickWorkflows(SPREAD, at(h, m));
      most = Math.max(most, due.length);
      for (const w of due) {
        count.set(w, (count.get(w) || 0) + 1);
        if (w === "model-radar.yml") assert.equal(m % 10, 5);
      }
    }
  }
  assert.equal(most, 2);
  for (const w of workflows(SPREAD)) assert.equal(count.get(w), 144, w);
  assert.equal(count.get("model-sst.yml"), 24);
});

test("the run count asks for in_progress and queued with per_page=1 and adds the totals", async () => {
  const f = api({ inProgress: 3, queued: 2 });
  assert.equal(await activeRuns(ENV, f), 5);
  assert.equal(f.calls.length, 2);
  for (const [i, s] of [[0, "in_progress"], [1, "queued"]]) {
    const { url, init } = f.calls[i];
    assert.equal(url, `https://api.github.com/repos/Allshore-JM/wave-app/actions/runs?status=${s}&per_page=1&exclude_pull_requests=true`);
    assert.equal(init.method, "GET");
    assert.equal(init.headers.Authorization, "Bearer t0k3n");
    assert.equal(init.headers.Accept, "application/vnd.github+json");
    assert.ok(init.headers["User-Agent"]);
  }
  await assert.rejects(activeRuns(ENV, api({ countStatus: 403 })), /count in_progress: HTTP 403/);
  await assert.rejects(activeRuns(ENV, api({ countBody: "<html>" })), /unreadable/);
  for (const body of ['{"workflow_runs":[]}', '{"total_count":"3"}', '{"total_count":-1}', '{"total_count":1.5}', "null"]) {
    await assert.rejects(activeRuns(ENV, api({ countBody: body })), /total_count|unreadable/, body);
  }
});

test("the cap: below it the due workflows go; at or above it they wait for their next minute", async () => {
  const env = { ...SPREAD, GH_MAX_ACTIVE: "8" };
  assert.equal(maxActive(env), 8);
  let f = api({ inProgress: 6, queued: 1 });                      // 7 active: room for one
  let r = await tick(env, f, at(17, 5));                          // radar + the hourly SST due
  assert.deepEqual(r.sent, ["model-radar.yml"]);
  assert.deepEqual(r.held, ["model-sst.yml"]);
  assert.equal(r.active, 7);
  assert.deepEqual(f.sent(), ["model-radar.yml"]);
  f = api({ inProgress: 8 });                                     // exactly at the cap: nothing goes
  r = await tick(env, f, at(17, 0));
  assert.deepEqual(r.sent, []);
  assert.deepEqual(r.held, ["model-frames.yml"]);
  assert.deepEqual(f.sent(), []);
  f = api({ inProgress: 5, queued: 7 });                          // queued runs count too
  r = await tick(env, f, at(17, 0));
  assert.deepEqual(f.sent(), []);
  f = api({ inProgress: 2 });
  r = await tick(env, f, at(17, 0));
  assert.deepEqual(f.sent(), ["model-frames.yml"]);
  assert.deepEqual(r.held, []);
  // the count failing never stops a dispatch
  for (const bad of [{ countStatus: 500 }, { countBody: "x" }]) {
    f = api(bad);
    r = await tick(env, f, at(17, 0));
    assert.deepEqual(f.sent(), ["model-frames.yml"]);
    assert.match(r.countError, /count in_progress/);
    assert.equal(r.active, null);
  }
  const thrower = async (url) => {
    if (url.includes("/actions/runs?")) throw new TypeError("fetch failed");
    return { status: 204, text: async () => "" };
  };
  r = await tick(env, thrower, at(17, 0));
  assert.deepEqual(r.sent, ["model-frames.yml"]);
  assert.match(r.countError, /fetch failed/);
  // no cap set: no count at all
  f = api({ inProgress: 99 });
  r = await tick(SPREAD, f, at(17, 0));
  assert.equal(f.counts(), 0);
  assert.deepEqual(f.sent(), ["model-frames.yml"]);
  for (const [v, want] of [["8", 8], ["", 0], [undefined, 0], ["x", 0], ["-1", 0], ["0", 0]]) {
    assert.equal(maxActive({ GH_MAX_ACTIVE: v }), want, String(v));
  }
});

test("a dispatch failing under the cap still throws (the tick shows as an error); the held ones are never tried", async () => {
  const env = { ...SPREAD, GH_MAX_ACTIVE: "8" };
  const f = api({ inProgress: 7, dispatchStatus: 401 });
  await assert.rejects(tick(env, f, at(17, 5)), /dispatch model-radar[.]yml@Live-Buoy-Update: HTTP 401/);
  assert.deepEqual(f.sent(), ["model-radar.yml"]);                // the SST one was held, never tried
  await assert.rejects(tick({ ...env, GH_WORKFLOW: " , " }, f, at(17, 5)), /names no workflow/);
});

test("scheduled: one minute's tick logs what went, what waited and why", async () => {
  const logs = [];
  const savedLog = console.log;
  const savedFetch = globalThis.fetch;
  const env = { ...SPREAD, GH_MAX_ACTIVE: "8" };
  console.log = (s) => logs.push(s);
  try {
    globalThis.fetch = api({ inProgress: 7 });
    await worker.scheduled({ cron: "* * * * *", scheduledTime: at(17, 5).getTime() }, env);
    globalThis.fetch = api({ countStatus: 502 });
    await worker.scheduled({ cron: "* * * * *", scheduledTime: at(17, 6).getTime() }, env);
    globalThis.fetch = api();
    await worker.scheduled({ cron: "* * * * *", scheduledTime: at(17, 9).getTime() }, { ...env, GH_WORKFLOW: "model-frames.yml", GH_MINUTE: "" });
  } finally {
    console.log = savedLog;
    globalThis.fetch = savedFetch;
  }
  assert.match(logs[0], /^dispatched model-radar[.]yml@Live-Buoy-Update; held model-sst[.]yml: 7 runs active [(]cap 8[)] [(]cron [*] [*] [*] [*] [*][)]$/);
  assert.match(logs[1], /^dispatched model-gfsspec[.]yml@Live-Buoy-Update; count failed [(]count in_progress: HTTP 502[)]; dispatched anyway/);
  assert.match(logs[2], /^nothing due/);
});

test("the deployed configuration: every minute, ten workflows spread, radar pinned to :x5, a cap of 8", () => {
  const cfg = readFileSync(new URL("./wrangler.jsonc", import.meta.url), "utf8");
  const v = (k) => (cfg.match(new RegExp(`"${k}":\\s*"([^"]*)"`)) || [])[1];
  assert.match(cfg, /"crons":\s*\["[*] [*] [*] [*] [*]"\]/);
  const env = { GH_WORKFLOW: v("GH_WORKFLOW"), GH_PERIOD: v("GH_PERIOD"), GH_MINUTE: v("GH_MINUTE"), GH_HOURLY: v("GH_HOURLY"), GH_MAX_ACTIVE: v("GH_MAX_ACTIVE") };
  assert.equal(env.GH_WORKFLOW, TEN);
  assert.equal(periodOf(env), 10);
  assert.equal(maxActive(env), 8);
  const m = minutesOf(env);
  assert.equal(m.get("model-radar.yml"), 5);
  assert.equal(new Set(m.values()).size, 10);                     // one workflow per minute
  for (let h = 0; h < 2; h++) {
    for (let mm = 0; mm < 60; mm++) assert.ok(tickWorkflows(env, at(h, mm)).length >= 1);
  }
});

test("a minute with nothing due asks GitHub nothing (no count, no dispatch)", async () => {
  const env = { ...ENV, GH_WORKFLOW: "model-frames.yml", GH_PERIOD: "10", GH_MAX_ACTIVE: "8" };
  const f = api({ inProgress: 0 });
  const r = await tick(env, f, at(17, 3));                        // model-frames.yml is due at minute 0 only
  assert.deepEqual(r.due, []);
  assert.equal(f.calls.length, 0);
  const g = api({ inProgress: 0 });
  assert.deepEqual((await tick(env, g, at(17, 10))).sent, ["model-frames.yml"]);
  assert.equal(g.counts(), 2);
});
