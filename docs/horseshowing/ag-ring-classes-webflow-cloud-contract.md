# AG Ring Classes Webflow Cloud Contract

Date: `2026-07-08`
Status: `current ring-classes contract`

## Purpose

This document applies `docs/horseshowing/ag-form-airtable-webflow-cloud-handoff.md` to the `ring-classes` AG output.

`ring-classes` is a report/print output. It is not an Airtable writeback form.

## Contract Layers

```text
Airtable AG reference rows
  -> document ring-classes intent, allowed fields, and allowed endpoints

Git source
  -> executable CSS and JS assets for the AG output

Webflow Cloud
  -> serves a stable loader through a stable route

Webflow embed
  -> loads the Webflow Cloud route into the Webflow page
```

## Git Source

Local prototype:

```text
prototypes/horseshowing/ring-classes-ring-name-group-prototype.html
```

Locked local source:

```text
prototypes/horseshowing/ring-classes-ring-name-group-LOCKED-2026-07-08.html
```

Executable CSS asset:

```text
webflow/ring-classes/ring-classes.css
```

Executable JS asset:

```text
webflow/ring-classes/ring-classes.js
```

Webflow Cloud loader source:

```text
webflow-cloud-test/src/assets/ring-classes/source.html
```

The loader source must stay small. It mounts the approved shell and loads CDN CSS/JS assets. Do not put the full editable template back into this file.

Webflow Cloud route:

```text
webflow-cloud-test/src/pages/ring-classes.js
```

Expected deployed route:

```text
https://ringstatus.com/test/ring-classes
```

CDN asset base:

```text
https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@{assetVersion}/webflow/ring-classes
```

Current loader default:

```text
assetVersion = window.RS_RING_CLASSES_ASSET_VERSION || "main"
```

When a commit is approved/pushed, pin `RS_RING_CLASSES_ASSET_VERSION` or the loader default to the approved commit SHA.

## Endpoint Contract

The AG output loads the same endpoint chain as the original ring group reference:

Primary:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/wec_live_grid/execute?action=wec-mobile-live
```

Fallback:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-mobile-live
```

URL params are forwarded:

```text
show_no
focus_day
```

## Display Mapping

Header title:

```text
Ring Classes · Show {show_no} · {focus_date}
```

Header subtitle and status:

```text
{source} · show {show_no} · {focus_date} · updated {last_updated}
```

Ring anchors:

```text
ring_name_normalized -> ring group
ring_display -> visible anchor label
```

Rows:

```text
time
display_ring
class_number
class_name
entry_count
status
horse_items
```

## Enabled Option Packs

```text
3column-print
focus
special-rows-without-underlying-data
anchorby-ring_name_normalized
filterby-barn_name
```

`Horse` only hides/shows `action-bar-bottom`. It does not run filtering by itself.

## Webflow Embed

```html
<div id="rs-ring-classes-loader"></div>
<script>
(() => {
  const root = document.getElementById("rs-ring-classes-loader");
  const params = new URLSearchParams(window.location.search);
  const endpoint = "https://ringstatus.com/test/ring-classes";
  const url = new URL(endpoint);
  if (params.get("show_no")) url.searchParams.set("show_no", params.get("show_no"));
  if (params.get("focus_day")) url.searchParams.set("focus_day", params.get("focus_day"));

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

  url.searchParams.set("_", String(Date.now()));

  fetch(url.toString(), { cache: "no-store" })
    .then((response) => {
      if (!response.ok) throw new Error(`Ring classes failed ${response.status}`);
      return response.text();
    })
    .then((html) => {
      root.innerHTML = html;
      return runScripts(root);
    })
    .catch((error) => {
      root.innerHTML = `<div style="padding:12px;font:14px Arial,sans-serif;color:#111;">Ring classes failed to load: ${String(error.message || error)}</div>`;
    });
})();
</script>
```

## Writeback Boundary

No writeback is enabled for this output.

If writeback is added later, it must follow:

```text
browser -> Webflow Cloud -> Airtable
```

Never browser -> Airtable.

## Verification Gates

Before this is treated as valid:

| Gate | Required Proof |
|---|---|
| Loader source | `webflow-cloud-test/src/assets/ring-classes/source.html` stays a small shell/CDN loader. |
| CSS asset | `webflow/ring-classes/ring-classes.css` contains the approved locked styling. |
| JS asset | `webflow/ring-classes/ring-classes.js` contains the approved behavior/data wiring. |
| Route | `/test/ring-classes` returns HTML with `cache-control: no-store`. |
| Endpoint | Loaded page consumes current WEC endpoint payload. |
| Metadata | Title/subtitle map from payload metadata. |
| Anchors | Ring anchors group by `ring_name_normalized`. |
| Filters | `Horse` only shows/hides `action-bar-bottom`; barn buttons filter rows when clicked. |
| Print | Print sheet uses 3 columns and current visible state. |
| Writeback | No writeback exposed. |
