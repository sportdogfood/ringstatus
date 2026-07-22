# `rs_content_actions` generation procedure

Date: 2026-07-20

Status: Home-proven generation contract for controlled extension to additional page tables

## Purpose

This procedure creates the actionable content queue in Airtable from the page-specific raw node ledgers. It reproduces the method proven on `home` while preserving exact page, section, source-record, card, tag, and Webflow element identities.

This procedure only creates Airtable records in `rs_content_actions`. It does not edit Webflow, run the Webflow page-change consumer, mark actions complete, or publish.

## Fixed Airtable identities

- Base: `appDN3R51ZPmwgMib`
- Destination table: `rs_content_actions`
- Destination table ID: `tblboGuCCADDo54Hm`
- Page registry: `rs_pages_index`
- Section registry: `section-mcp`

Never substitute a table name for an Airtable ID in an API call. Read the live schema at the beginning of every run and resolve every field ID before writing.

## Approved page scope

Process one page per run, in this order unless the operator explicitly selects a smaller scope.

| Run order | Page key | Source table ID | Webflow page ID |
| ---: | --- | --- | --- |
| 1 | `home` | `tblFFRbXGyXJL8OUr` | `6982268d7543ac3c801512cd` |
| 2 | `contact` | `tblWenBJaHAgMHY6d` | `69c6d5166b6358d0d31391bb` |
| 3 | `company` | `tblsfDFY4IMNbZefx` | `6a5c1f45186a3fba417c0dd7` |
| 4 | `me` | `tblOxaTUUuMiZ2dna` | `69f90b3bafbe82c8b46c317c` |
| 5 | `show` | `tbl3ydOEUHwpaxmtF` | `6a5c251fbbf4d4b2a17bd5a8` |
| 6 | `ride` | `tblUHjk6O00Hfe9w4` | `6a5c22fd3e9f56a933419796` |
| 7 | `horses` | `tbl1XEFwrHj10x5Vh` | `6a5c22ecab67c490e7b0ca71` |
| 8 | `tack` | `tbl3B0hxTHkfIyO9E` | `6a5c231be35f7d14435ebb25` |
| 9 | `waze` | `tblPRZum8a5iStXkf` | `6a5c23f67752640f03bfb26e` |

`home` is the control case. Its existing 90 actions are verification evidence, not permission to insert duplicates.

## Required source fields

Stop the page run if its source table does not expose all required fields:

- `raw_key`
- `section_index`
- `section_element_id`
- `parent_element_id`
- `element_id`
- `depth`
- `sibling_index`
- `element_type`
- `ignore`
- `focus`
- `text_content`
- `section-mcp`
- `card_root_element_id`
- `tag_group_element_id`
- `webflow_page_id`

## Required destination fields

Stop the page run if `rs_content_actions` does not expose all required fields:

- `action_key`
- `page`
- `page_sort`
- `mcp`
- `mcp_sort`
- `main`
- `main_sort`
- `card`
- `card_iter`
- `tag`
- `tag_iter`
- `element_id`
- `text_content`
- `source_table`
- `source_record_id`
- `actionable`
- `action_status`
- `created_at`
- `rs_page_blocks`

## Page preflight

Complete all gates before deriving any candidate action.

1. Read the live `rs_pages_index` record for the exact page key.
2. Require exactly one page record.
3. Require `active = active` and `is_ks3 = checked` when those fields exist on the live page registry.
4. Require the registry's `webflow_page_id` to equal the ID in the approved scope table above.
5. Require the registry to link to the expected source table.
6. Read every source row for the page, following Airtable pagination until no cursor remains.
7. Require every populated source `webflow_page_id` to equal the registry page ID.
8. Read existing destination actions for the page before generating the dry run.
9. Build sets of existing `action_key` and `source_record_id` values. Neither may be duplicated.

Any failed gate is `FAIL`; write nothing for that page.

## Candidate eligibility

A source row becomes a candidate only when every condition is true:

- `focus` is checked;
- `ignore` is not checked;
- `text_content` is present after testing for an empty string;
- `element_id` is present;
- exactly one `section-mcp` record is linked;
- the linked MCP is active for that page;
- the row is a visitor-facing text node, not a structural wrapper or media node;
- `raw_key`, `element_id`, and Airtable source record ID are unique within the page source table.

Do not silently trim, rewrite, concatenate, or generate `text_content`. Preserve the exact source value.

Exclude punctuation-only rows, required-field stars, controls, navigation/social labels, credits, and known static template labels. At minimum, exclude these exact values:

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

## Deterministic hierarchy derivation

Do not use Airtable record order as DOM order.

1. Reconstruct the source tree using `element_id` and `parent_element_id`.
2. Partition rows by linked `section-mcp` and `section_index`.
3. Within each section, traverse the reconstructed tree in preorder, ordering siblings by `sibling_index`.
4. Assign `mcp_sort` from the page's active MCP order, not from an inactive kitchen-sink section.
5. For each eligible row:
   - `main` = exact `element_id`;
   - `main_sort` = one-based eligible-text preorder within the MCP;
   - `card` = exact `card_root_element_id`, blank when outside a card;
   - `card_iter` = one-based first-appearance order of distinct card roots within the MCP, blank outside a card;
   - `tag` = exact `tag_group_element_id`, blank when outside a tag group;
   - `tag_iter` = one-based eligible-text preorder within that tag group, blank outside a tag group.

Every tag row must retain its containing card identity. A populated `tag` with a blank `card` is a preflight failure unless the source tree proves the tag group is intentionally outside every card.

## Destination mapping

Create one destination record per eligible source row.

| Destination field | Exact value |
| --- | --- |
| `action_key` | `<page_key>:<two-digit source section_index>:<element_id>`; replace a Webflow-page-ID prefix in `raw_key` with the stable page key |
| `page` | Exact linked `rs_pages_index` record ID |
| `page_sort` | Approved page order shown in this procedure |
| `mcp` | Exact linked `section-mcp` record ID from the source row |
| `mcp_sort` | Derived active MCP order |
| `main` | Exact source `element_id` |
| `main_sort` | Derived eligible-text order within the MCP |
| `card` | Exact source `card_root_element_id`, or blank |
| `card_iter` | Derived card order, or blank |
| `tag` | Exact source `tag_group_element_id`, or blank |
| `tag_iter` | Derived tag-text order, or blank |
| `element_id` | Exact source `element_id` |
| `text_content` | Exact source `text_content` |
| `source_table` | Exact page key/source table name |
| `source_record_id` | Exact Airtable source record ID |
| `actionable` | checked |
| `action_status` | `queued` |
| `created_at` | One UTC timestamp shared by the entire page batch |
| `rs_page_blocks` | Exact active page-block record for this page and MCP |

## Dry-run manifest

Dry run is mandatory and must not write Airtable records.

For the selected page, report:

| Gate | Required result |
| --- | --- |
| Source rows | Total rows read after pagination |
| Focus | Count with `focus` checked |
| Ignore | Count excluded by `ignore` |
| Empty text | Count excluded for empty `text_content` |
| Static exclusions | Count and exact values excluded by policy |
| Missing MCP | Count; must be zero among candidates |
| Inactive MCP | Count; must be zero among candidates |
| Candidates | Final eligible count |
| Cards | Distinct card roots by MCP |
| Tags | Distinct tag roots and tag-text counts by card |
| Existing actions | Existing count for this page |
| Duplicate action keys | Count; must be zero |
| Duplicate source IDs | Count; must be zero |
| Planned creates | Candidates absent from the destination |

Also output the complete planned records sorted by:

```text
page_sort ASC
mcp_sort ASC
main_sort ASC
card_iter ASC
tag_iter ASC
action_key ASC
```

The operator must approve the dry-run manifest before apply.

## Apply procedure

Apply is a separate, explicitly approved operation.

1. Re-read the live source and destination tables immediately before applying.
2. Recompute the manifest; do not reuse stale candidate data.
3. Stop if source counts, candidate identities, text, page identity, or existing destination keys differ from the approved manifest.
4. Create records in batches no larger than the current Airtable API/MCP limit; default to ten records per request unless live tool help states otherwise.
5. Use destination field IDs, not field names, in record payloads.
6. After every batch, read back the created record IDs and exact mapped fields.
7. Stop on the first failed batch. Do not retry through another endpoint and do not continue to the next page.
8. Never update or delete an existing action as part of generation.

## Readback and PASS gate

After all planned creates for one page:

1. Read all destination actions linked to the page.
2. Require the destination count to equal `existing actions + planned creates`.
3. Require every planned `action_key` exactly once.
4. Require every planned `source_record_id` exactly once.
5. Compare every written mapping field to the approved manifest.
6. Require `actionable = checked` and `action_status = queued` on every new record.
7. Confirm no record was created for an ignored, empty, inactive, structural, or static-exclusion row.

Return `PASS` only when every planned record passes exact readback. Otherwise return `FAIL`, list the mismatched record IDs/action keys, and do not advance to the next page.

## Page-by-page rollout

The nine pages are nine bounded runs. A PASS on one page does not authorize the next page automatically.

```text
home control verification
  -> contact dry run -> approval -> apply -> readback
  -> company dry run -> approval -> apply -> readback
  -> me dry run -> approval -> apply -> readback
  -> show dry run -> approval -> apply -> readback
  -> ride dry run -> approval -> apply -> readback
  -> horses dry run -> approval -> apply -> readback
  -> tack dry run -> approval -> apply -> readback
  -> waze dry run -> approval -> apply -> readback
```

Do not run page generation concurrently. Sequential runs keep the source count, deduplication set, batch timestamp, and failure boundary attributable to one page.

## Home control verification

Before extending the procedure, verify the existing Home control without writing:

- exactly 90 `rs_content_actions` records linked to Home;
- all 90 share `created_at = 2026-07-19T20:36:14.037Z`;
- record creation occurred between `2026-07-19T20:36:15Z` and `2026-07-19T20:36:18Z`;
- action keys and source record IDs are unique;
- every action resolves back to one Home source record;
- no excluded static value is present as an independently actionable record.

The status distribution may change as a separate consumer processes actions; status counts are not a generation-integrity gate. Generation must never reset completed or failed actions to queued.

## Required final report per page

```text
PAGE: <page_key>
SOURCE TABLE: <table_id>
PAGE RECORD: <record_id>
WEBFLOW PAGE ID: <page_id>
SOURCE ROWS: <count>
ELIGIBLE: <count>
EXCLUDED: <count by reason>
EXISTING ACTIONS: <count>
PLANNED CREATES: <count>
CREATED: <count>
READBACK MATCHED: <count>
DUPLICATES: 0
RESULT: PASS | FAIL
WEBFLOW CHANGES: NONE
PUBLISHED: NO
```

## Prohibited shortcuts

- Do not create an action from every non-empty text node.
- Do not infer focus from element type or visible words.
- Do not ignore the source `ignore` checkbox.
- Do not flatten cards or tag groups.
- Do not use page names where Airtable record links are required.
- Do not derive replacement copy from `raw_node_json` or neighboring nodes.
- Do not treat action generation as proof that a Webflow consumer or scheduled workflow is running.
- Do not write to `rs_content_edits` or `rs_edit_drafts` during this procedure.
- Do not edit Webflow or publish.
