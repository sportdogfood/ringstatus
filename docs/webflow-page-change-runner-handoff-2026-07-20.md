# Webflow page-change runner handoff

Date: 2026-07-20

Status: current connection and implementation contract

## Purpose

This handoff explains how a new RingStatus runner can take an approved Airtable content action, locate the exact native Webflow text node, change that node through the official Webflow connection, verify the change by readback, and close the Airtable action.

The runner changes native Webflow page content. It does not replace page HTML, rebuild section structure, alter styles, or publish unless a separate instruction explicitly authorizes publishing.

## Current state

- Webflow site: `ringstatus`
- Webflow site ID: `6982268b7543ac3c80151266`
- Airtable base ID: `appDN3R51ZPmwgMib`
- Action table: `rs_content_actions`
- Action table ID: `tblboGuCCADDo54Hm`
- `home` is the only page currently seeded into the action table.
- Home currently has 90 actionable records covering 10 active MCP sections, 12 card groups, and 3 tag groups.
- Every seeded Home action currently has `actionable = checked` and `action_status = queued`.
- The page-specific raw/derived tables exist for all 12 pages listed below.
- No production scheduled runner has been proved from this table yet. The table and records are preparation, not evidence that automation is running.
- No Webflow publish was performed while preparing this contract.

## Hard operating boundary

Codex prepares, inspects, patches approved repeatable code, and verifies exact gates. The production runner executes the business process.

Do not use manual calls as proof that a scheduled runner works. Do not keep retrying writes until state changes. If the approved runner path fails, record the failure and stop. Do not publish as part of a text update unless publishing is separately and explicitly authorized.

## Connection map

```text
rs_content_actions queued record
  -> linked rs_pages_index record
  -> webflow_page_id
  -> exact element_id
  -> Webflow native text update
  -> Webflow element readback
  -> rs_content_actions status update
```

Supporting hierarchy:

```text
page
  -> mcp
    -> main / main_sort
      -> card / card_iter
        -> tag / tag_iter
```

The hierarchy is for deterministic sorting and visual grouping. The write target remains the exact `element_id` on the exact linked page.

## How to connect

### Codex or MCP-capable runner

Use the authenticated Airtable and Webflow MCP connections exposed in the session. Never place credentials in prompts, source files, browser JavaScript, Airtable cells, or logs.

Webflow connection preflight:

1. Confirm the Webflow MCP tools are exposed.
2. Call `webflow_guide_tool` once for the session before any other Webflow tool.
3. Call the Webflow sites tool with `list_sites`.
4. Require exactly the expected `ringstatus` site ID: `6982268b7543ac3c80151266`.
5. Search site-specific Webflow agent instructions after the site ID is known and follow any returned rules.
6. Use the current tool schema. Do not guess action names or payload fields from this document.

Airtable connection preflight:

1. Confirm Airtable access to base `appDN3R51ZPmwgMib`.
2. Read the live table schema before processing records.
3. Require `rs_content_actions` table ID `tblboGuCCADDo54Hm`.
4. Resolve linked record IDs through the live schema. Never substitute display names for Airtable IDs.
5. Read only actionable queued records for the page or bounded batch being processed.

### Standalone runner

If the runtime is not MCP-capable, use approved server-side Webflow and Airtable credentials supplied through the deployment environment. Do not expose either token to a public Webflow page or browser bundle. The standalone implementation must preserve the same preflight, exact-ID, readback, stop, and no-publish gates described here.

## Page registry

`rs_pages_index` is the page identity source. The runner must follow the action's linked `page` record and read `webflow_page_id`; the table below is a current reference, not permission to bypass the link.

| Page key | Webflow page ID | Page table ID |
| --- | --- | --- |
| `home` | `6982268d7543ac3c801512cd` | `tblFFRbXGyXJL8OUr` |
| `ks3` | `6a57dd9bb3e56ddd7968c250` | `tblp6Vo9XWnc3IxUJ` |
| `ks3-template` | `6a5a8ebd5ac27ec9a32093af` | `tblTk9bBmQSQJrNjX` |
| `contact` | `69c6d5166b6358d0d31391bb` | `tblWenBJaHAgMHY6d` |
| `me` | `69f90b3bafbe82c8b46c317c` | `tblOxaTUUuMiZ2dna` |
| `waze` | `6a5c23f67752640f03bfb26e` | `tblPRZum8a5iStXkf` |
| `ride` | `6a5c22fd3e9f56a933419796` | `tblUHjk6O00Hfe9w4` |
| `show` | `6a5c251fbbf4d4b2a17bd5a8` | `tbl3ydOEUHwpaxmtF` |
| `horses` | `6a5c22ecab67c490e7b0ca71` | `tbl1XEFwrHj10x5Vh` |
| `tools` | `6a5c233770221493caf81508` | `tbl3tvOpMCM8p1Aru` |
| `tack` | `6a5c231be35f7d14435ebb25` | `tbl3B0hxTHkfIyO9E` |
| `company` | `6a5c1f45186a3fba417c0dd7` | `tblsfDFY4IMNbZefx` |

## Action table contract

One `rs_content_actions` record represents one visitor-facing text node that may be written independently.

| Field | Contract |
| --- | --- |
| `action_key` | Stable unique action identity: page, section order, and element ID |
| `page` | Link to the owning `rs_pages_index` record |
| `page_sort` | Page-order helper |
| `mcp` | Link to the owning `section-mcp` record |
| `mcp_sort` | Active section order on the page |
| `main` | Exact actionable text element ID used for the main ordering level |
| `main_sort` | DOM-derived order of actionable text within the MCP section |
| `card` | `card_root_element_id`; blank outside a card |
| `card_iter` | One-based card order inside the MCP section |
| `tag` | `tag_group_element_id`; blank outside a tag group |
| `tag_iter` | One-based tag-text order inside the tag group |
| `element_id` | Exact Webflow element ID to read and write |
| `text_content` | Desired visitor-facing text for the Webflow text node |
| `source_table` | Page-specific source table name |
| `source_record_id` | Exact source Airtable record ID used to produce the action |
| `actionable` | Must be checked before processing |
| `action_status` | `queued`, `processing`, `complete`, or `failed` |
| `created_at` | Queue insertion timestamp |

`text_content` is the canonical desired value for the action. The runner must not derive replacement copy from raw JSON, a parent wrapper, neighboring text, an SEO field, or a generic template.

## Source-table contract

The page-specific tables mirror the section-frame node shape and include:

- exact `element_id` and `parent_element_id`
- `section-mcp` link
- `focus` and `ignore`
- `card_root_element_id`
- `tag_group_element_id`
- `text_content`
- exact hierarchy order fields

Only records with `focus = checked`, `ignore` not checked, and non-empty `text_content` may become actions.

The original `ks3-raw` table remains raw evidence. Do not rewrite `ks3-raw` to make a Webflow update appear successful. See `docs/ks3_raw_contract.md` and `docs/ks3_style_contract.md` for the raw evidence boundary.

## Content that must never enter the action queue

Do not create actions for ignored records, wrappers, images, punctuation-only nodes, required-field stars, global social/navigation labels, credits, or placeholder controls.

Known excluded exact values include:

```text
.
*
Facebook
Instagram
X
YouTube
Made by Lainey Posa
Access RingStatus
Login to RingStatus
Request a Demo
Tab 1
Tab 2
```

An exclusion is not permission to delete the Webflow node. It means the node is not an independently scalable content input.

## Card and tag grouping

`card_root_element_id` is derived from the nearest explicit repeated Webflow card wrapper. It groups every descendant belonging to one card.

`tag_group_element_id` is derived from the nearest `rs-blog-tag-row` ancestor. It groups the tag-row wrapper and its descendant tag nodes.

Current KS3 proof:

| MCP | Card/tag groups | Tag strings |
| --- | ---: | ---: |
| `mcp-pwr-slider` | 6 | 24 |
| `mcp-timeline` | 3 | 12 |

Every tag action must retain both its containing `card` and its `tag` group. Do not flatten tags from different cards into one list.

## Runner processing sequence

Process a bounded batch in this exact order.

### 1. Claim one queued action

Require all of the following:

- `actionable = checked`
- `action_status = queued`
- one linked `page`
- one linked `mcp`
- non-empty `element_id`
- non-empty `text_content`
- unique `action_key`

Set `action_status = processing` only after the record passes preflight.

### 2. Resolve the page

Follow the linked `page` record to `rs_pages_index` and read `webflow_page_id`.

Fail if:

- the page link is missing or ambiguous;
- `webflow_page_id` is blank;
- the page ID is not returned by the connected RingStatus site;
- the action's `source_table` does not match the linked page key.

### 3. Read the exact Webflow element

Use Webflow's native element data tool against the resolved site ID and page ID. Query the exact `element_id`.

Require:

- exactly one element match;
- a text-capable element or String node;
- the element belongs to the resolved page;
- the target is not a component definition requiring an unrecorded component scope;
- the target is not ignored by the source record.

Do not locate a write target by matching its current words. Text is not identity.

### 4. Apply the text

Use Webflow's native element text action with the exact `element_id` and the action's exact `text_content`.

Do not:

- write to the parent wrapper when the action targets a String child;
- combine sibling String and Span content into a replacement heading;
- change styles, attributes, links, images, visibility, heading level, or hierarchy;
- alter another action in the same card or tag group;
- publish.

### 5. Read back from Webflow

Query the same page and exact `element_id` again.

PASS requires the returned text to equal `text_content` exactly. Normalize neither punctuation nor whitespace unless the approved runner contract explicitly defines that normalization.

If readback fails or differs:

1. Set `action_status = failed`.
2. Record the actual error in the runner log.
3. Stop processing that action.
4. Do not retry through a different endpoint or write to another element.

### 6. Close the action

Only after exact Webflow readback:

1. Set `action_status = complete`.
2. Record runner execution evidence in the runner log.
3. Leave `action_key`, hierarchy, source identity, and `text_content` unchanged.

The current action table does not yet contain dedicated attempt, error, applied-at, or Webflow-readback fields. A production runner should add an append-only execution log rather than overloading `text_content` or raw source tables with audit data.

## Batch ordering

Use this deterministic sort:

```text
page_sort ASC
mcp_sort ASC
main_sort ASC
card_iter ASC
tag_iter ASC
action_key ASC
```

Blank card or tag iteration means the action is outside that nested structure. It is not an error.

## Publish boundary

Native Webflow edits remain staged until Webflow is published. Text readback from the element data surface proves the Designer value changed; it does not prove the public page changed.

Default behavior is **do not publish**.

Publishing requires a separate explicit instruction naming the site or page and intended domains. If publishing is authorized, use the approved Webflow publish tool and then verify the public rendered page separately. Never treat a Designer/API readback as public-page proof.

## Required verification report

For every bounded run, report:

| Gate | Required evidence |
| --- | --- |
| Connection | RingStatus site ID and Airtable base/table IDs matched |
| Input | Count of claimed actionable queued records |
| Identity | Every action resolved to one linked page and one exact element ID |
| Write | Count of successful Webflow text writes |
| Readback | Count exactly matching `text_content` |
| Failure | Failed action keys and stop reasons |
| Airtable close | Count set to `complete` only after readback |
| Publish | `NOT PUBLISHED`, unless separately authorized and verified |

PASS means every claimed action passed every required gate. Any missing identity, ambiguous element, failed write, mismatched readback, or premature completion is FAIL.

## Safe first test for a new runner

Do not begin with all 90 Home actions.

1. Select one reviewed Home action whose text change is harmless and reversible.
2. Confirm the linked page is `home` and the page ID is `6982268d7543ac3c801512cd`.
3. Read the exact element before writing.
4. Apply the approved `text_content` without publishing.
5. Read the exact element back.
6. Close only that action.
7. Return the verification report and stop.

Scaling to a larger batch requires explicit approval after the single-action path passes.

## Do-not-do list

- Do not infer page IDs, element IDs, or MCP records from names alone.
- Do not scrape the published page to choose a write target.
- Do not write a whole heading when only one String node is actionable.
- Do not use `card_root_element_id` or `tag_group_element_id` as a Webflow text target.
- Do not create actions from every non-empty String without applying `focus` and `ignore`.
- Do not rewrite raw tables as success evidence.
- Do not mark `complete` before Webflow readback.
- Do not substitute a manual/direct call for proof of the scheduled runner.
- Do not publish without separate explicit approval.

## First-read references

```text
docs/webflow-page-change-runner-handoff-2026-07-20.md
docs/ks3_raw_contract.md
docs/ks3_style_contract.md
docs/codex_runner_integrations_handoff_2026-06-19.md
```

## Starter instruction for the next runner

```text
Read docs/webflow-page-change-runner-handoff-2026-07-20.md completely before acting. Verify the live Airtable schema and the Webflow RingStatus site; do not rely on remembered tool payloads. Process only explicitly approved rs_content_actions records. Resolve each linked page and exact element_id, read before writing, set only the exact text_content, read back from Webflow, and mark complete only after exact equality. Do not publish. If any required identity, write, or readback gate fails, mark the action failed, report FAIL, and stop that action. Manual calls do not prove a scheduled runner.
```
