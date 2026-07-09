# AG Form Airtable + Webflow Cloud Handoff

Date: `2026-07-08`
Status: `handoff reference`
Primary base source: `prototypes/horseshowing/ag-base-shell-reset.html`
Related reset handoff: `docs/horseshowing/ag-base-shell-reset-handoff.md`
Related AG contract: `docs/horseshowing/ag-output-system-contract.md`
Locked `rs-schedules` input/output method: `docs/horseshowing/rs-schedules-webflow-input-output-method-contract.md`

## Purpose

This handoff explains how Airtable, Git, Webflow Cloud, and Webflow embeds work together for AG-based RingStatus forms, reports, and lists.

The goal is to prevent drift.

Airtable documents the intent and configuration reference.
Git holds the actual source files.
Webflow Cloud serves the deployable HTML/API route.
Webflow embeds load the Webflow Cloud route into the Webflow page.

Do not treat any one layer as the whole system.

## Layer Model

```text
Airtable AG reference rows
  -> document grid/form intent, allowed fields, endpoints, and notes

Git source files
  -> hold the actual HTML/CSS/JS, routes, docs, and embed snippets

Webflow Cloud / Astro
  -> serves the AG output through a stable route

Webflow page embed
  -> loads the Webflow Cloud route into the Webflow page
```

## Airtable Role

Airtable is used as a reference/documentation layer for AG outputs.

It is not the first source of executable UI truth. It keeps details honest so the next build does not restart from memory or screenshots.

Known Airtable tables:

| Table | Role |
|---|---|
| `ag_grids` | Grid/form/report-level reference row. |
| `ag_tables_allowed` | Source tables allowed for a grid/form/report. |
| `ag_fields_allowed` | Fields allowed or expected by a grid/form/report. |
| `ag_end_points_allowed` | Endpoint URLs or endpoint references allowed for a grid/form/report. |

## `ag_grids` Record Use

Each AG form/report should have a row in `ag_grids`.

That row should document:

- grid/form name
- active status
- source/prototype file
- Webflow Cloud route
- Webflow embed root id
- intended page/use case
- stack blocks used
- action bars used
- anchors used
- print enabled or not
- input/writeback enabled or not
- hide/diff/focus behavior
- expected columns
- required field labels
- style/base notes
- comments for suggested changes

The row does not need to make the UI fully dynamic before the UI is useful.

The first job is documentation and repeatability.

## Allowed Tables / Fields / Endpoints

Use linked/reference rows to keep each output honest:

| Table | What To Record |
|---|---|
| `ag_tables_allowed` | Which Airtable/source tables the output may read or write. |
| `ag_fields_allowed` | Which fields are expected, displayed, hidden, editable, searchable, printed, or submitted. |
| `ag_end_points_allowed` | Which Webflow Cloud/Catalyst/Slate/API endpoint powers the output. |

These tables are guardrails.

They should answer:

- what data is this output allowed to show?
- what data can the user edit?
- what endpoint should Webflow load?
- what fields are display-only?
- what fields are payload/writeback fields?
- what fields are search/filter/anchor fields?

## Git Role

Git remains the source of executable truth.

For the current reset base:

```text
prototypes/horseshowing/ag-base-shell-reset.html
```

For Webflow Cloud serving:

```text
webflow-cloud-test/src/assets/ag-base-shell/source.html
webflow-cloud-test/src/pages/ag-base-shell.js
```

For documentation:

```text
docs/horseshowing/ag-base-shell-reset-handoff.md
docs/horseshowing/ag-output-system-contract.md
docs/horseshowing/ag-form-airtable-webflow-cloud-handoff.md
```

Do not let Airtable notes silently replace the Git source. If Airtable changes the contract, update the source or docs explicitly.

## Webflow Cloud Role

Webflow Cloud serves the AG output through Astro.

Current route source:

```text
webflow-cloud-test/src/pages/ag-base-shell.js
```

Current route implementation:

```js
import html from "../assets/ag-base-shell/source.html?raw";

export const GET = async () => new Response(html, {
  headers: {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store"
  }
});
```

Current source HTML imported by that route:

```text
webflow-cloud-test/src/assets/ag-base-shell/source.html
```

Expected deployed route:

```text
https://ringstatus.com/test/ag-base-shell
```

The route serves HTML. The Webflow page does not need the full form pasted into it once this route is deployed.

## Webflow Embed Role

The Webflow embed should stay small and stable.

It loads the Webflow Cloud route, inserts the returned HTML into a root div, and runs scripts inside the loaded HTML.

Current root id:

```text
rs-ag-base-shell-loader
```

Current endpoint:

```text
https://ringstatus.com/test/ag-base-shell
```

Current full embed:

```html
<div id="rs-ag-base-shell-loader"></div>
<script>
(() => {
  const root = document.getElementById("rs-ag-base-shell-loader");
  const endpoint = "https://ringstatus.com/test/ag-base-shell";

  function runScripts(scope) {
    const scripts = Array.from(scope.querySelectorAll("script"));
    return scripts.reduce((chain, oldScript) => chain.then(() => new Promise((resolve, reject) => {
      const script = document.createElement("script");
      for (const attr of oldScript.attributes) script.setAttribute(attr.name, attr.value);
      if (oldScript.src) {
        script.onload = resolve;
        script.onerror = reject;
        script.src = oldScript.src;
      } else {
        script.textContent = oldScript.textContent;
      }
      oldScript.replaceWith(script);
      if (!script.src) resolve();
    })), Promise.resolve());
  }

  fetch(`${endpoint}?_=${Date.now()}`, { cache: "no-store" })
    .then((response) => {
      if (!response.ok) throw new Error(`AG base shell failed ${response.status}`);
      return response.text();
    })
    .then((html) => {
      root.innerHTML = html;
      return runScripts(root);
    })
    .catch((error) => {
      root.innerHTML = `<div style="padding:12px;font:14px Arial,sans-serif;color:#111;">AG base shell failed to load: ${String(error.message || error)}</div>`;
    });
})();
</script>
```

## Build Flow

Use this flow for a new AG form/report:

1. Create or update the Airtable `ag_grids` row.
2. Document allowed tables in `ag_tables_allowed`.
3. Document expected fields in `ag_fields_allowed`.
4. Document the endpoint in `ag_end_points_allowed`.
5. Build or update the local prototype in Git.
6. Copy the approved source into `webflow-cloud-test/src/assets/.../source.html`.
7. Create or update the Webflow Cloud route under `webflow-cloud-test/src/pages/`.
8. Run the Webflow Cloud build.
9. Deploy Webflow Cloud only when approved.
10. Paste or update the small Webflow embed only when the endpoint contract changes.

## Writeback Rule

If an AG form writes data:

```text
Browser AG form
  -> Webflow Cloud route
  -> Airtable API
  -> Airtable record/change log
  -> response back to AG form
```

Never put Airtable tokens in browser JS or Webflow embeds.

The browser may submit form data. Webflow Cloud owns validation, field allowlists, Airtable writes, and change logging.

For `rs-schedules` project output, input and published render must use the Packing-style method locked in `docs/horseshowing/rs-schedules-webflow-input-output-method-contract.md`:

```text
Webflow root/config
  -> pinned CSS/JS assets from Git/CDN
  -> Webflow Cloud state/action route
  -> Airtable/API read or write
  -> frontend refresh/render
```

`webflow/packing-worksheet/styles.css` is the shared stylesheet surface for this family. Class names may be renamed or cleaned up, but new `rs-schedules` pages should not fork into one-off styling or the injected full-HTML Barn Entry loader pattern.

## What Airtable Should Not Do Yet

Do not make the entire UI dynamic from Airtable before the base is stable.

Airtable should not be used as an excuse to:

- invent endless fields
- make every CSS value dynamic
- bypass Git source files
- bypass Webflow Cloud
- bypass endpoint verification
- skip browser proof

Use Airtable first as a contract/reference layer.

## Verification Gates

Before treating a Webflow-connected AG form as valid:

| Gate | Required Proof |
|---|---|
| Airtable reference | `ag_grids` row documents the form/report intent. |
| Fields | Expected fields are listed or clearly documented. |
| Endpoint | Webflow Cloud route is known and returns `200`. |
| Embed | Webflow embed points to the intended route/root id. |
| Source | Git file matches the routed Webflow Cloud source. |
| Browser | Webflow/local browser renders the expected form/report. |
| Print | Print path works if print is enabled. |
| Writeback | Writes go through Webflow Cloud, not browser-to-Airtable. |

## Summary

Use Airtable to keep the form/report contract honest.

Use Git to hold the actual implementation.

Use Webflow Cloud to serve the current source through a stable endpoint.

Use Webflow embed as a small loader.

This keeps AG forms repeatable without rebuilding the whole Webflow page or guessing from screenshots.
