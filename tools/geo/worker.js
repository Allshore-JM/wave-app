// Cloudflare Worker: where a visitor is, from the IP address (plan section 46). The site opens at the visitor's own
// area with the nearest forecast station, without asking the browser for its location (owner, 2026-10-17: "if I
// travel the site should open where I am"). Cloudflare places every request it receives (request.cf: latitude,
// longitude, city, region, country); the site itself runs on Render, which has no such data, so the page asks this
// Worker once per fresh tab: GET https://geo.allshoresurf.com/ -> {"lat":21.3,"lon":-157.9,"city":...}.
//
// Nothing is stored or logged by this code; the answer is never cached (no-store) and only the site's own origins
// may read it from a page (Access-Control-Allow-Origin is echoed for the allow-list; others get no CORS header, so
// their pages cannot read the body). City-level accuracy at best; a VPN places the visitor at its exit.
// Plain var ALLOW_ORIGINS (wrangler.jsonc): extra origins, comma-separated (e.g. a preview site). No secrets.

export const DEFAULT_ORIGINS = [
  "https://allshoresurf.com",
  "https://www.allshoresurf.com",
  "https://wave-app-clean.onrender.com",
];

export function origins(env) {
  const extra = String((env && env.ALLOW_ORIGINS) || "").split(",").map((s) => s.trim()).filter(Boolean);
  return DEFAULT_ORIGINS.concat(extra);
}

function num(v) {
  if (v === null || v === undefined || v === "") return NaN;
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}

function text(v) {
  return typeof v === "string" && v ? v.slice(0, 80) : null;
}

// The whole answer as plain data (the tests call this; the handler only wraps it in a Response).
export function answer(request, env) {
  const headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "Vary": "Origin",
    "X-Robots-Tag": "noindex",
    "X-Content-Type-Options": "nosniff",
  };
  const origin = request.headers.get("Origin");
  if (origin && origins(env).includes(origin)) headers["Access-Control-Allow-Origin"] = origin;
  if (request.method === "OPTIONS") {
    headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS";
    headers["Access-Control-Max-Age"] = "86400";
    return { status: 204, headers, body: null };
  }
  if (request.method !== "GET" && request.method !== "HEAD") {
    headers["Allow"] = "GET, HEAD, OPTIONS";
    return { status: 405, headers, body: JSON.stringify({ error: "GET only" }) };
  }
  if (new URL(request.url).pathname !== "/") return { status: 404, headers, body: JSON.stringify({ error: "not found" }) };
  const cf = request.cf || {};
  const lat = num(cf.latitude), lon = num(cf.longitude);
  if (Number.isNaN(lat) || Number.isNaN(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return { status: 503, headers, body: JSON.stringify({ error: "no location" }) };
  }
  const body = { lat, lon, city: text(cf.city), region: text(cf.region), country: text(cf.country) };
  return { status: 200, headers, body: request.method === "HEAD" ? null : JSON.stringify(body) };
}

export default {
  async fetch(request, env) {
    const a = answer(request, env);
    return new Response(a.body, { status: a.status, headers: a.headers });
  },
};
