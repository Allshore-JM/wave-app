// node --test worker.test.js  (no network: requests are built by hand)
import { test } from "node:test";
import assert from "node:assert/strict";
import worker, { answer, origins, DEFAULT_ORIGINS } from "./worker.js";

const HONOLULU = { latitude: "21.30690", longitude: "-157.85830", city: "Honolulu", region: "Hawaii", country: "US" };

function req({ method = "GET", path = "/", origin = "https://allshoresurf.com", cf = HONOLULU, nocf = false } = {}) {
  const headers = new Headers();
  if (origin) headers.set("Origin", origin);
  const r = new Request("https://geo.allshoresurf.com" + path, { method, headers });
  if (!nocf) Object.defineProperty(r, "cf", { value: cf });
  return r;
}

test("the site's origins get the visitor's position with CORS and no caching", () => {
  for (const origin of DEFAULT_ORIGINS) {
    const a = answer(req({ origin }), {});
    assert.equal(a.status, 200);
    assert.equal(a.headers["Access-Control-Allow-Origin"], origin);
    assert.equal(a.headers["Cache-Control"], "no-store");
    assert.equal(a.headers["Vary"], "Origin");
    assert.equal(a.headers["X-Robots-Tag"], "noindex");
    assert.deepEqual(JSON.parse(a.body), { lat: 21.3069, lon: -157.8583, city: "Honolulu", region: "Hawaii", country: "US" });
  }
});

test("another origin, or none, gets no CORS header (its page cannot read the answer)", () => {
  for (const origin of ["https://evil.example", "https://allshoresurf.com.evil.example", "http://allshoresurf.com", null]) {
    const a = answer(req({ origin }), {});
    assert.equal(a.status, 200);
    assert.equal(a.headers["Access-Control-Allow-Origin"], undefined, String(origin));
  }
});

test("ALLOW_ORIGINS adds origins and never removes the site's own", () => {
  const env = { ALLOW_ORIGINS: " https://preview.example , ,https://two.example" };
  assert.deepEqual(origins(env), DEFAULT_ORIGINS.concat(["https://preview.example", "https://two.example"]));
  assert.equal(answer(req({ origin: "https://preview.example" }), env).headers["Access-Control-Allow-Origin"], "https://preview.example");
  assert.deepEqual(origins({}), DEFAULT_ORIGINS);
  assert.deepEqual(origins(undefined), DEFAULT_ORIGINS);
});

test("no usable position is a 503 with a JSON error, never a made-up place", () => {
  assert.equal(answer(req({ nocf: true }), {}).status, 503);         // a request Cloudflare did not place
  for (const cf of [null, {}, { latitude: "", longitude: "" }, { latitude: "abc", longitude: "1" },
    { latitude: "91", longitude: "0" }, { latitude: "0", longitude: "181" }, { latitude: null, longitude: null }]) {
    const a = answer(req({ cf }), {});
    assert.equal(a.status, 503, JSON.stringify(cf));
    assert.deepEqual(JSON.parse(a.body), { error: "no location" });
    assert.equal(a.headers["Cache-Control"], "no-store");
  }
  const zero = answer(req({ cf: { latitude: "0", longitude: "0" } }), {});
  assert.equal(zero.status, 200);                                   // the Gulf of Guinea is a real answer
  assert.deepEqual(JSON.parse(zero.body), { lat: 0, lon: 0, city: null, region: null, country: null });
});

test("only GET / and HEAD /; OPTIONS answers a preflight; other paths are 404", () => {
  assert.equal(answer(req({ method: "POST" }), {}).status, 405);
  assert.equal(answer(req({ method: "PUT" }), {}).headers["Allow"], "GET, HEAD, OPTIONS");
  const pre = answer(req({ method: "OPTIONS" }), {});
  assert.equal(pre.status, 204);
  assert.equal(pre.body, null);
  assert.equal(pre.headers["Access-Control-Allow-Origin"], "https://allshoresurf.com");
  assert.equal(pre.headers["Access-Control-Allow-Methods"], "GET, HEAD, OPTIONS");
  const head = answer(req({ method: "HEAD" }), {});
  assert.equal(head.status, 200);
  assert.equal(head.body, null);
  assert.equal(answer(req({ path: "/x" }), {}).status, 404);
  assert.equal(answer(req({ path: "/?k=1" }), {}).status, 200);
});

test("long strings from cf are cut and non-strings dropped", () => {
  const a = answer(req({ cf: { ...HONOLULU, city: "x".repeat(500), region: 42, country: "" } }), {});
  const body = JSON.parse(a.body);
  assert.equal(body.city.length, 80);
  assert.equal(body.region, null);
  assert.equal(body.country, null);
});

test("the fetch handler wraps the answer in a Response", async () => {
  const res = await worker.fetch(req(), {});
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("Access-Control-Allow-Origin"), "https://allshoresurf.com");
  assert.equal(res.headers.get("Content-Type"), "application/json; charset=utf-8");
  assert.equal((await res.json()).city, "Honolulu");
  const none = await worker.fetch(req({ cf: {} }), {});
  assert.equal(none.status, 503);
});
