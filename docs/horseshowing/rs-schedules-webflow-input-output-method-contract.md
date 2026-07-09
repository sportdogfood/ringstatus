# RS Schedules Webflow Input/Output Method Contract

Date: `2026-07-09`
Status: `LOCKED METHOD`

## Purpose

This document locks the Webflow integration method for `rs-schedules` project output, whether the page accepts user input or only renders published data.

The locked method is the Packing method:

```text
Webflow embed
  -> root element + page config
  -> pinned CDN CSS/JS assets from Git
  -> Webflow Cloud/API route
  -> Airtable read/write or published payload
  -> frontend refresh/render
```

The Barn Entry injected full-HTML loader is not the locked method for new input or published `rs-schedules` output.

## Locked Method

All `rs-schedules` input or published render surfaces should use this pattern:

1. Webflow page contains a small stable embed.
2. Embed creates or targets a root element.
3. Embed sets page config such as API/action/state/health URLs.
4. Embed loads pinned CSS and JS assets from Git/CDN.
5. Browser JS calls Webflow Cloud/API routes.
6. Webflow Cloud owns Airtable tokens, validation, writes, logs, and response payloads.
7. Frontend renders or refreshes from the returned payload.

The browser must not write directly to Airtable.

## Current Source References

| Role | File |
|---|---|
| Webflow embed example | `webflow/packing-worksheet/wec-packing-webflow-embed.html` |
| Local preview example | `webflow/packing-worksheet/wec-packing-webflow-preview.html` |
| Frontend app JS | `webflow/packing-worksheet/wec-packing.js` |
| Shared project stylesheet | `webflow/packing-worksheet/styles.css` |
| Locked/reset stylesheet | `webflow/packing-worksheet/rsa-stylesheets.locked.css` |
| Webflow Cloud route index | `webflow-cloud-test/src/pages/wec-packing/index.js` |
| Webflow Cloud state route | `webflow-cloud-test/src/pages/wec-packing/state.js` |
| Webflow Cloud action route | `webflow-cloud-test/src/pages/wec-packing/action.js` |
| Webflow Cloud health route | `webflow-cloud-test/src/pages/wec-packing/health.js` |
| Webflow Cloud shared server logic | `webflow-cloud-test/src/lib/wec-packing.js` |

## Stylesheet Lock

`webflow/packing-worksheet/styles.css` is the shared stylesheet surface for `rs-schedules` project output.

This applies to:

- input forms
- review screens
- published render pages
- mobile output
- print-oriented render pages when they share the same visual language

Class names may be renamed or cleaned up when needed, but the styling authority remains this shared stylesheet pattern. Do not create a separate one-off stylesheet for each `rs-schedules` page unless the exception is documented and approved.

Use page-specific CSS only for page-specific behavior or layout that cannot belong to the shared surface.

## Input Rule

For input, use the Packing method:

```text
Webflow root/config
  -> frontend JS captures user input
  -> POST action route
  -> server validates allowlisted action/fields
  -> server writes Airtable
  -> server writes audit/change log when configured
  -> server returns fresh state
  -> frontend rerenders
```

Do not use the injected full-HTML Barn Entry loader as the input method.

## Published Render Rule

For published render, use the same asset/config/API shape:

```text
Webflow root/config
  -> frontend JS loads current state/payload
  -> Webflow Cloud/API route returns normalized data
  -> frontend renders current page
```

Published render pages may be read-only, but they still follow the same root/config/CDN asset/API route contract.

## Not Locked For New `rs-schedules` Output

The following pattern remains a reference only and is not the locked method for new `rs-schedules` input/output work:

```text
Webflow embed
  -> fetch full HTML route
  -> inject returned HTML into page
  -> clone styles/scripts from returned document
  -> run injected scripts
```

Reference files for that older method:

| Role | File |
|---|---|
| Barn Entry source HTML | `webflow-cloud-test/src/assets/barn-entry/source.html` |
| Barn Entry full-HTML route | `webflow-cloud-test/src/pages/barn-entry.js` |

## Do Not Drift Rules

| Rule | Meaning |
|---|---|
| Do not paste full app HTML into Webflow | Keep the Webflow embed small and stable. |
| Do not use browser Airtable tokens | All Airtable reads/writes go through Webflow Cloud/API routes. |
| Do not create one-off CSS per page | Start from `webflow/packing-worksheet/styles.css`. |
| Do not fork the input method | New input follows root/config/CDN assets/API action routes. |
| Do not fork the published-render method | Read-only pages still use the same root/config/CDN assets/API state routes. |
| Do not treat Barn Entry injected HTML as the new base | It is reference only for this project direction. |

