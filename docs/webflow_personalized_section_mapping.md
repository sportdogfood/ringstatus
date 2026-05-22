# Webflow Personalized Section Mapping

## Confirmed Webflow Contract

Site:

```text
site name: ringstatus
siteId: 6982268b7543ac3c80151266
```

Active Designer page inspected through MCP:

```text
page_id: 6a0da8d5a96eb92e7bdc4269
pageName: hps_8778
slug: hps-8778
mode: design
branch: none
```

Existing active page root inspected through MCP:

```text
element_id: { component: 6a0da8d5a96eb92e7bdc4269, element: feb3272d-57bb-0e74-ccda-adf0631b9169 }
classes: hps-app
domId: none in Designer settings
attributes: none
children include: th-hps-opener, th-hps-module, lp-summary-row lp-shell-footer th-hps-status-footer, lp-modal
```

Existing HPS browser mount from Git:

```text
root selector: #hps-app
embed file: webflow/hps/hps-8778-webflow-embed.html
renderer: webflow/hps/hps.js
api route: webflow-cloud-test/src/pages/hps/horses.js
```

No existing personalized-section element, root ID, or data slot was present on the active Designer page during this pass. The code below is therefore wired as a Git/Webflow Cloud contract and is ready to attach to an existing Designer-owned container once its element/root selector is provided or added by the designer.

## Personalized Section Contract

Target selector:

```text
#rs-personalized-section
```

Input context fields:

```json
{
  "season": "fall",
  "tags": ["running", "hiking", "fall"]
}
```

Output fields:

```json
{
  "ok": true,
  "season": { "key": "fall", "label": "Fall" },
  "copy": {
    "headline": "I love when its Fall season",
    "intro": "In the Fall I like to do these activities"
  },
  "activities": [
    { "title": "Trail running", "description": "...", "tags": ["running", "fall"] }
  ],
  "fallback": ""
}
```

Fallback/loading/error text:

```text
Loading personalized content...
No activities matched this profile yet.
Personalized content failed to load.
```

## Data Source

Selected source:

```text
Git JSON
```

Reason:

```text
The requested scenario uses static curated season/activity content and no inline edit/writeback was requested.
```

Static dataset:

```text
webflow/personalized-section/personalized-section-content.json
```

Server filtering endpoint:

```text
POST https://ringstatus.webflow.io/test/personalized-section/content
GET  https://ringstatus.webflow.io/test/personalized-section/content?season=fall&tags=running,hiking,fall
```

Request body:

```json
{
  "datasetUrl": "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/personalized-section/personalized-section-content.json",
  "context": {
    "season": "fall",
    "tags": ["running", "hiking", "fall"]
  }
}
```

Inline edit/writeback:

```text
Not enabled.
```

If writeback is later requested, browser code must POST through Astro/Webflow Cloud to Airtable. Do not expose Airtable tokens in Webflow embeds.

## Files

Frontend renderer:

```text
webflow/personalized-section/personalized-section.js
```

Embed contract:

```text
webflow/personalized-section/personalized-section-webflow-embed.html
```

Astro route:

```text
webflow-cloud-test/src/pages/personalized-section/content.js
```

Shared filter helper:

```text
webflow-cloud-test/src/lib/personalized-content.js
```

Behavior test:

```text
webflow-cloud-test/test/personalized-content.test.js
```

## Webflow Placement Still Needed

Before publishing, confirm the real Designer-owned container for this section:

```text
page_id
section_id / element_id
root selector or root div ID
class names
typography/styles
content slot ownership
```

Do not rebuild the section shell unless explicitly approved.
