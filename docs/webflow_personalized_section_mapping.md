# Webflow Personalized Section Mapping

## Proven Embed Test Group

This document groups the current Webflow embed tests that prove the same RingStatus pattern:

```text
Webflow-owned container
  -> stable root selector / data attributes
  -> browser renderer
  -> server endpoint
  -> normalized JSON
  -> paint into existing template slots
```

These are UI/data-wiring tests. They are not scheduled workflow proof and do not replace approved cadence/runner verification.

## Test Review Matrix

Each test proves a different slice of the same Webflow data-painting architecture.

| Test | Unit Painted | Data Source Shape | Webflow Contract | What It Proves | Graduation Path |
| --- | --- | --- | --- | --- | --- |
| Personalized section | Content section | `season -> activities[]` from Git JSON or endpoint JSON | `#rs-personalized-section` | Context-aware filtering from tags/season can paint copy and rows into a section shell. | Use Git JSON or Catalyst/static endpoint for stable content; use Airtable as editor staging before export. |
| Value filler POC | Scalar text values | `counts` object | `[data-rs-value]` slots inside `#rs-poc-value-filler` | Existing text elements can be filled without cloning templates or replacing page HTML. | Fold into a generic value binder for counters, labels, status cards, and summary strips. |
| Counts summary snippet | Scalar text values | `counts` object | `[data-rs-value]` slots anywhere in scope | The same value-fill pattern works even without a formal model/root wrapper. | Prefer the explicit `value_filler` root for repeatable contracts. |
| Template repeater POC | Repeated table rows | `data.rows || data.horses` | hidden `[data-rs-template]` row inside `[data-rs-list]` | Webflow can own a row template while JS clones and fills it from endpoint data. | Fold into a generic repeater binder, with explicit list/template keys and predictable empty/error states. |
| Horse kits grid snippet | Derived repeated table rows | `horses`, `kits`, `kitItems`, `packingRows` | `rs-airtable-grid`, `horse_rows`, `horse_row` | Browser-side join/count logic can derive display rows from Airtable-shaped collections. | Move derivation server-side or into the reusable bridge when logic becomes shared. |
| WEC packing bridge | Mixed values and repeated rows | normalized horse-kits endpoint payload | `[data-rs-packing-bridge]`, `[data-rs-value]`, `[data-rs-list]`, `[data-rs-template]` | Scalar filling and row repeating can be combined in one reusable renderer. | Promote as the preferred renderer for WEC horse-kits-style sections. |
| Page hierarchy renderer | Full page HTML | `{ ok, html }` | `#rs-page-root`, `pageKey` | One Webflow root can be replaced by a server-rendered page hierarchy. | Use only when the unit is a full custom RingStatus page, not a small component. |

Key distinction:

```text
value filler = fill existing nodes
template repeater = clone existing template nodes
personalized section = filter data then paint a section
bridge = reusable mixed binder
page hierarchy = replace root with server-rendered page HTML
```

## Method Reference

| Name | overview.md | handoff.md | Method | Endpoint | When to use |
| --- | --- | --- | --- | --- | --- |
| Personalized section | `docs/webflow_personalized_section_mapping.md` | `docs/webflow_cloud_dataset_template_handoff.md` | Filter context fields, then paint copy and list rows into an existing section root. | `GET/POST https://ringstatus.webflow.io/test/personalized-section/content` | Use when the Webflow section shell already exists and content changes by `season`, `tags`, profile, or preference context. |
| Value filler | `docs/webflow_personalized_section_mapping.md` | `docs/webflow_cloud_dataset_template_handoff.md` | Fill existing `[data-rs-value]` text slots from scalar JSON fields. | `GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one` | Use for counters, summary numbers, labels, and status values where no row cloning is needed. |
| Counts summary snippet | `docs/webflow_personalized_section_mapping.md` | `docs/webflow_cloud_dataset_template_handoff.md` | Fill loose `[data-rs-value]` slots from a `counts` object without requiring a formal root wrapper. | `GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one` | Use as the smallest scalar proof or quick embed test; graduate to `Value filler` for repeatable section contracts. |
| Template repeater | `docs/webflow_personalized_section_mapping.md` | `docs/webflow_cloud_dataset_template_handoff.md` | Clone a hidden `[data-rs-template]` node inside a `[data-rs-list]`, then fill child `[data-rs-value]` slots. | `GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one` | Use when Webflow owns one row/card template and the data layer needs to repeat it. |
| Dynamic section renderer | `docs/webflow_custom_pages_system_final_overview.md` | `docs/webflow-dynamic-render-landing-page-handoff.md` | Read one page/block section from Airtable `rscom`, render section HTML, and return it to a Webflow-owned root. | `GET https://ringstatus.com/test/rs-dynamic-section/content?pageKey=home&blockKey=...` | Use when the unit is one Airtable-managed Webflow section and the section hierarchy/content still lives in `rscom`. |
| Horse kits grid | `docs/wec_packing_comprehensive_overview_2026-06-06.md` | `docs/wec_packing_project_overview_handoff.md` | Build display rows from Airtable-shaped `horses`, `kits`, `kitItems`, and `packingRows`, then paint a Webflow table template. | `GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one` | Use for WEC packing views that need derived kit counts per horse. |
| WEC packing bridge | `docs/wec_packing_comprehensive_overview_2026-06-06.md` | `docs/wec_packing_current_codex_handoff_2026-05-31.md` | Reusable browser bridge that combines scalar value filling and row-template repeating under `[data-rs-packing-bridge]`. | `GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one` | Use as the preferred reusable renderer for WEC horse-kits-style Webflow sections. |
| Page hierarchy renderer | `docs/webflow_custom_pages_system_final_overview.md` | `docs/webflow-dynamic-render-landing-page-handoff.md` | Fetch server-rendered HTML for a `pageKey`, then replace one Webflow root's inner HTML. | `GET https://ringstatus.com/test/rs-page-render?pageKey=rs_home` | Use when the unit is an entire RingStatus custom page, not a small component or section. |

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

## WEC Horse Kits Bridge Tests

The two shared WEC snippets belong to this same proven embed group. They test Airtable-backed data painting into Webflow-owned markup.

Endpoint:

```text
GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one
```

Server route:

```text
webflow-cloud-test/src/pages/wec-packing/horse-kits.js
```

Renderer:

```text
webflow/packing-worksheet/wec-packing-bridge.js
```

Embed contract:

```text
webflow/packing-worksheet/wec-packing-bridge-webflow-embed.html
```

### Test 1: Horse Kits Grid

Designer-owned markup:

```text
table.rs-airtable-grid[data-rs-table="horse_kits"]
tbody[data-rs-list="horse_rows"]
tr[data-rs-template="horse_row"]
```

Rendered row slots:

```text
row_number
horse_name
profile_url via data-rs-href="profile_url"
kit_label
need
packed
left
```

Source JSON collections consumed by the browser test:

```text
horses
kits
kitItems
packingRows
```

Client-side row logic:

```text
horse.pakKitItemIds -> kitItems
kit.kitItemIds -> matched kit
packingRows -> packed/not_needed counts
need = active items minus not_needed
left = need minus packed
```

This proves that Airtable-shaped endpoint data can paint into a table template without rebuilding the Webflow table.

### Test 2: Counts Summary

Designer-owned slots:

```text
[data-rs-value="horse_count"]
[data-rs-value="kit_item_count"]
[data-rs-value="touched_count"]
```

Endpoint fields:

```text
counts.horses
counts.kitItems
counts.packingRows
```

This proves that simple scalar endpoint values can paint into existing Webflow text elements using `data-rs-value`.

### Test 2a: Value Filler POC

The value filler snippet is the explicit model-key version of the counts summary test.

Root contract:

```text
section#rs-poc-value-filler
data-rs-model="value_filler"
data-rs-embed-key="wec_horse_kits_counts_values"
```

Designer-owned value slots:

```text
strong[data-rs-value="horse_count"]
strong[data-rs-value="kit_item_count"]
strong[data-rs-value="touched_count"]
```

Endpoint:

```text
GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one
```

Field mapping:

```text
counts.horses -> horse_count
counts.kitItems -> kit_item_count
counts.packingRows -> touched_count
```

Status attributes:

```text
data-rs-status="loading"
data-rs-status="ready"
data-rs-status="failed"
```

This POC proves the minimal scalar model:

```text
static Webflow text element
  -> data-rs-value key
  -> endpoint scalar
  -> textContent update
```

### Test 3: Template Repeater POC

The template repeater snippet is another variant of the same concept. It gives the Webflow container a model key and embed key, then repeats a hidden row template from endpoint data.

Root contract:

```text
section#rs-poc-template-repeater
data-rs-model="template_repeater"
data-rs-embed-key="wec_horse_kits_rows_template"
```

Designer-owned table:

```text
table.rs-airtable-grid[data-rs-table="horse_kits"]
tbody[data-rs-list="horse_rows"]
tr[data-rs-template="horse_row"]
```

Rendered row slots:

```text
row_number
horse_name
kit_label
need
packed
left
```

Endpoint:

```text
GET https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one
```

Status attributes:

```text
data-rs-status="loading"
data-rs-status="ready"
data-rs-status="failed"
```

Data fallback rule in the POC:

```text
rows = data.rows || data.horses || []
```

This POC proves the minimal repeater model:

```text
template row hidden in Webflow
  -> fetch endpoint JSON
  -> clone template
  -> fill [data-rs-value] cells
  -> append rendered rows
```

Compared with `wec-packing-bridge.js`, this POC is simpler and less normalized. The bridge is the better reusable renderer; the POC is useful as the smallest proof of the Webflow template-repeater contract.

## RS Page Hierarchy Renderer Test

This shared snippet belongs to the same group, but at page scope instead of section/table scope. It proves that Webflow can own one page root while the server returns a complete rendered page hierarchy.

Root contract:

```text
div#rs-page-root
data-rs-model="page_hierarchy_renderer"
data-rs-page-key="rs_home"
```

Endpoint:

```text
GET https://ringstatus.com/test/rs-page-render?pageKey=rs_home
```

Server route:

```text
webflow-cloud-test/src/pages/rs-page-render.js
```

Existing system docs:

```text
docs/webflow_custom_pages_system_final_overview.md
docs/webflow-dynamic-render-landing-page-handoff.md
```

Response contract:

```json
{
  "ok": true,
  "html": "<main class=\"rs-page\" data-rs-page=\"rs_home\">...</main>"
}
```

Client behavior:

```text
set #rs-page-root status to loading
fetch page render endpoint with pageKey
replace root.innerHTML with response html
set status to ready or failed
```

Status attributes:

```text
data-rs-status="loading"
data-rs-status="ready"
data-rs-status="failed"
```

This proves the highest-level variant of the same concept:

```text
Webflow-owned root
  -> page key
  -> server-rendered page hierarchy
  -> HTML injected into root
```

Use this when the unit being painted is an entire RingStatus custom page, not a small section, scalar count, or row repeater.

### Merged Bridge Contract

The reusable bridge combines both tests:

```text
root selector: [data-rs-packing-bridge]
config: window.WEC_PACKING_BRIDGE_CONFIG
endpointUrl: https://ringstatus.com/test/wec-packing/horse-kits?packWaveKey=wave_one&viewKey=wave_one
```

Supported value slots include:

```text
report_title
report_subtitle
pack_wave_label
horse_count
visible_horse_count
kit_count
kit_item_count
touched_count
need_count
packed_count
left_count
not_needed_count
```

Supported list slot:

```text
data-rs-list="horse_rows"
```

Supported row template:

```text
data-rs-template="horse_row"
```

The bridge should be attached only to a confirmed Webflow page/container by stable identifiers:

```text
page_id
element_id
root selector or root DOM/data attribute
```

Do not target by visual position alone.

## Data Source Positioning

Non-time-sensitive content:

```text
Git JSON or Catalyst/static endpoint
```

Editor-managed staging:

```text
Airtable
```

Logic/rules may be staged in Airtable during editing. Approved stable data/rules can be exported or pushed to Git for faster runtime reads.

Writeback:

```text
Browser edit UI -> Webflow Cloud/Astro or Catalyst endpoint -> Airtable API
```

Do not put Airtable tokens in browser JS or Webflow embeds.
