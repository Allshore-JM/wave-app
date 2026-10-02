// node --test worker.test.js  (no network: fetch is stubbed)
import { test } from "node:test";
import assert from "node:assert/strict";
import worker, { dispatch, dispatchAll, workflows } from "./worker.js";

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
