# Webflow Cloud Dataset Template Handoff

This is the repeatable pattern proven by the LP History work. Use it when a Webflow page needs to render a static Git-hosted dataset and optionally save curation/enrichment edits back to Airtable without exposing the Airtable token in the browser.

## Working Reference

- Webflow visual page: `https://ringstatus.com/lph`
- Webflow Cloud test mount: `https://ringstatus.webflow.io/test`
- Health endpoint: `https://ringstatus.webflow.io/test/health`
- Enrichment endpoint: `https://ringstatus.webflow.io/test/lp-history/enrichment`
- Static frontend assets: `webflow/lp-history/`
- Server/API app: `webflow-cloud-test/`
- Last proven commit: `e4c4c1e Fix Webflow Cloud Astro env access`

Verified response from `/test/health`:

```json
{
  "ok": true,
  "service": "webflow-cloud-test",
  "enrichmentEndpoint": "/test/lp-history/enrichment",
  "env": {
    "hasAirtableToken": true,
    "hasAirtableBaseId": true,
    "table": "tbl58VhfgiMtYwB0I"
  }
}
```

Verified browser save message:

```text
Saved to Airtable at 8:49:06 PM (created, logged).
```

## Architecture

The browser renders from public static files in Git/CDN:

```text
Webflow Embed
  -> lp-history.css
  -> lp-history.js
  -> lp-history-history.json
  -> lp-history-layer.json
```

Edit mode writes through Webflow Cloud:

```text
Webflow page
  -> Webflow Cloud API route
  -> Airtable API
  -> primary enrichment record
  -> log enrichment record
```

Do not put Airtable tokens in the Webflow embed for production edit flows.

## Webflow Embed Pattern

Use one root div, config, stylesheet, and script.

```html
<div id="lp-history-app">Loading LP history...</div>

<script>
  window.LP_HISTORY_CONFIG = {
    historyUrl: "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history-history.json",
    layerUrl: "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history-layer.json",
    enrichmentUrl: "https://ringstatus.webflow.io/test/lp-history/enrichment"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/lp-history/lp-history.js"></script>
```

For a new template, keep the same shape and change:

- root id if needed
- asset folder
- `historyUrl`
- `layerUrl`
- `enrichmentUrl`

## Webflow Cloud App Requirements

The API app must be in its own folder and must be the folder Webflow Cloud deploys.

Current folder:

```text
webflow-cloud-test/
```

Required files:

```text
webflow-cloud-test/webflow.json
webflow-cloud-test/astro.config.mjs
webflow-cloud-test/package.json
webflow-cloud-test/src/pages/health.js
webflow-cloud-test/src/pages/lp-history/enrichment.js
```

`webflow.json`:

```json
{
  "cloud": {
    "framework": "astro"
  }
}
```

`astro.config.mjs` must use server output and avoid unnecessary generated bindings:

```js
import { defineConfig } from "astro/config";
import { sessionDrivers } from "astro/config";
import cloudflare from "@astrojs/cloudflare";

export default defineConfig({
  output: "server",
  adapter: cloudflare({
    imageService: "passthrough"
  }),
  session: {
    driver: sessionDrivers.lruCache()
  },
  security: {
    checkOrigin: false
  }
});
```

Astro 6 / Webflow Cloud env vars must be read this way:

```js
import { env } from "cloudflare:workers";
```

Do not use:

```js
locals.runtime.env
```

That fails in Astro 6 with:

```text
Astro.locals.runtime.env has been removed in Astro v6.
Use 'import { env } from "cloudflare:workers"' instead.
```

## Webflow Cloud Settings

In the Webflow Cloud project:

- Branch: `main`
- Path: current test path is `/test`
- Directory path: must point at the app folder if the app is not repo root

For the current implementation:

```text
Directory path = webflow-cloud-test
```

The Webflow Cloud **Path** is the public URL mount. It is not the Git folder.

## Required Environment Variables

Set these in the same Webflow Cloud environment that deploys the API app:

```text
AIRTABLE_TOKEN
AIRTABLE_BASE_ID
AIRTABLE_TABLE_LP
```

`AIRTABLE_TABLE_LP` can be the Airtable table ID, for example:

```text
tbl58VhfgiMtYwB0I
```

After changing env vars or directory settings, redeploy. If Webflow Cloud says `Needs deployment`, the running environment may not have the new settings yet.

## Test Order

After every deploy, test in this order:

1. App page:

```text
https://ringstatus.webflow.io/test
```

2. Runtime/env health:

```text
https://ringstatus.webflow.io/test/health
```

3. Enrichment GET:

```text
https://ringstatus.webflow.io/test/lp-history/enrichment
```

4. Browser edit save:

```text
Saved to Airtable at [time] (created, logged).
```

If `/test` works but `/test/health` fails, the problem is runtime/server code, not the visual page.

## Airtable Save Contract

The enrichment endpoint accepts records for:

```text
horse
competition
class
video
```

It upserts one primary record by `record_key`, then creates one log record for each save.

The endpoint currently filters outgoing Airtable fields through an allow-list in:

```text
webflow-cloud-test/src/pages/lp-history/enrichment.js
```

For another dataset, update that allow-list to match the new Airtable table exactly. Do not invent fields in code unless the Airtable table has them.

## Static Dataset Pattern

For another dataset/template:

1. Put static frontend files in a new folder:

```text
webflow/<template-name>/
```

2. Use these core files:

```text
<template-name>.css
<template-name>.js
<template-name>-history.json
<template-name>-layer.json
<template-name>-webflow-embed.html
```

3. Keep raw history separate from enrichment:

```text
history json = source facts
layer json = curation/enrichment defaults
Airtable = editable source of truth for saved enrichment
```

4. Normalize client-side only for display. Do not mutate the source history payload in the browser.

## Common Failure Modes

`Loading...` stays on screen:

- Usually frontend JS error or duplicate/missing root div.
- Check browser console first.

`/test` is 200 but API is 404:

- Route file is missing, wrong app folder deployed, or wrong URL mount.

`/test` is 200 but API is 500:

- Check Webflow Cloud runtime logs.
- In this project, the proven 500 was Astro 6 env access using the removed `locals.runtime.env`.

Health says env vars are false:

- Env vars are missing from that Webflow Cloud environment, or the environment needs redeploy.

Browser says saved locally only:

- Enrichment endpoint failed, endpoint URL is missing/wrong, or Airtable rejected the payload.

## Recommended Cleanup

The current folder name `webflow-cloud-test` is historical. For future clarity, rename or duplicate the pattern as:

```text
lp-history-cloud
```

For a production route, use a real API mount instead of `/test`, for example:

```text
/lph-api
```

Then the public page remains:

```text
/lph
```

and the save endpoint becomes:

```text
https://ringstatus.com/lph-api/lp-history/enrichment
```
