# KS3 raw section-tree contract

## Purpose

`ks3-raw` is a lossless node ledger for the section skeletons on the live Webflow `ks3` page. It records Webflow evidence only. It does not decide which nodes are content, dynamic, required, reusable, typography, or eligible for scaling.

## Extraction boundary

- Source site: `6982268b7543ac3c80151266` (`ringstatus`)
- Source page: `6a57dd9bb3e56ddd7968c250` (`/ks3`)
- Root selector: every Webflow node whose ordered classes include `rs5-section-frame`
- Coverage: each section-frame root and every descendant node, including blocks, DOM nodes, links, images, headings, paragraphs, spans, form nodes, and String children
- Excluded from the table: page-level wrappers, navigation, and other nodes outside the section-frame roots

## Raw-record rule

One Airtable record represents one Webflow node. Parent and child nodes remain separate records. The immediate parent ID, depth, sibling order, and section root ID preserve the exact Navigator hierarchy.

No node is omitted because of its class, content, visibility, apparent purpose, or location. In particular, `rs0-footer-credits`, `rs5-lainey-links`, and every descendant remain present exactly as returned by Webflow.

## Fields

| Field | Source |
|---|---|
| `raw_key` | Stable combination of page ID, section index, and element ID |
| `webflow_page_id` | Webflow page ID |
| `section_index` | One-based order of the section frame on the page |
| `section_element_id` | Element ID of the enclosing section-frame root |
| `element_id` | Exact Webflow node element ID |
| `parent_element_id` | Immediate parent node element ID; empty only for the section root |
| `depth` | Zero-based depth beneath the section root |
| `sibling_index` | Zero-based order under the immediate parent |
| `element_type` | Raw Webflow node type |
| `display_name` | Raw display name when returned by Webflow |
| `tag` | Raw resolved tag when returned |
| `class_names` | Ordered Webflow classes, one per line |
| `attributes_json` | Complete raw attributes JSON |
| `settings_json` | Complete raw settings JSON |
| `text_content` | Raw text returned on the node |
| `child_count` | Number of immediate child nodes |
| `raw_node_json` | Untouched non-recursive node properties returned by Webflow; child nodes are separate linked-by-ID records |
| `captured_at` | UTC extraction timestamp |

## Readable raw projections

For visual review in Airtable, selected values are also projected directly from `attributes_json`, `settings_json`, and the raw node identity. These are raw values, not classifications: `element_component_id`, `mcpid`, `data_rs_block`, `data_rs_enabled`, `data_rs_delivery`, `data_rs_value`, `dom_id`, `href`, `aria_label`, `html_role`, `visibility`, `asset_id`, `alt_text`, and `heading_level`.

## Prohibited interpretation

`ks3-raw` has no `required`, `active`, `is_ks3`, scope, exclusion, dynamic, front-facing, typography, content-role, or reuse fields. Those decisions belong in a separately reviewed derived contract and must never alter the raw capture.

The raw table is not scaled or cloned to other pages.
