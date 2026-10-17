# Deployment: allshore-geo (Cloudflare Workers Builds)

The Worker tells the site roughly where a visitor is (plan section 46): `GET https://geo.allshoresurf.com/` answers
`{"lat": 21.31, "lon": -157.86, "city": "Honolulu", "region": "Hawaii", "country": "US"}` from Cloudflare's own
placement of the request (`request.cf`), so the page can open at the visitor's area with the nearest forecast station,
without a browser location prompt. Nothing is stored; the answer is `Cache-Control: no-store`; only the site's own
origins may read it from a page (CORS allow-list in worker.js, extra ones in the `ALLOW_ORIGINS` var).

The Worker is deployed by Cloudflare's Git integration (Workers Builds), like `allshore-model-frames-trigger`.
Dashboard: Workers & Pages -> Create -> Import a repository -> Allshore-JM/wave-app.

| Setting | Value |
|---|---|
| Project name | `allshore-geo` (must equal `name` in `wrangler.jsonc`) |
| Root directory | `tools/geo` |
| Build command | `npm run build` (syntax check + the Node tests; must not be empty) |
| Deploy command | `npx wrangler deploy` (reads `wrangler.jsonc`: name, the custom domain, vars, no workers.dev URL) |
| Production branch | `Live-Buoy-Update` |
| Build watch paths | include `tools/geo/*` (pushes elsewhere in the repo do not build) |
| Preview builds | off |
| API token | a token for this Worker alone (Settings -> Builds -> API token -> Create new token) |

The custom domain `geo.allshoresurf.com` comes from `wrangler.jsonc` (`routes` with `custom_domain: true`): the first
deploy creates the DNS record (proxied) and the certificate on the allshoresurf.com zone. The site's own records stay
DNS-only (grey cloud): nothing about allshoresurf.com itself changes.

After the first deploy, check from a terminal:

    curl -s -H "Origin: https://allshoresurf.com" -D - https://geo.allshoresurf.com/

200, `access-control-allow-origin: https://allshoresurf.com`, `cache-control: no-store` and the JSON body. Then set
`GEO_BASE=https://geo.allshoresurf.com` on the Render service (test site first): the page asks the Worker only when
that variable is set; without it the site opens as before (Hawaii, station 51201).

To stop it: unset `GEO_BASE` on Render (the page stops asking at once), or delete the Worker. If the Worker is down
or slow, the page waits at most 2 s and then opens as before.
