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
  -> executable source for the AG output

Webflow Cloud
  -> serves the current AG source through a stable route

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

Webflow Cloud served source:

```text
webflow-cloud-test/src/assets/ring-classes/source.html
```

Webflow Cloud route:

```text
webflow-cloud-test/src/pages/ring-classes.js
```

Expected deployed route:

```text
https://ringstatus.com/test/ring-classes
```

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
| Source | `webflow-cloud-test/src/assets/ring-classes/source.html` matches locked source. |
| Route | `/test/ring-classes` returns HTML with `cache-control: no-store`. |
| Endpoint | Loaded page consumes current WEC endpoint payload. |
| Metadata | Title/subtitle map from payload metadata. |
| Anchors | Ring anchors group by `ring_name_normalized`. |
| Filters | `Horse` only shows/hides `action-bar-bottom`; barn buttons filter rows when clicked. |
| Print | Print sheet uses 3 columns and current visible state. |
| Writeback | No writeback exposed. |
