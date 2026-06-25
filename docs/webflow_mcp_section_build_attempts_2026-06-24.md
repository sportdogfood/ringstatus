# Webflow MCP Native Section Build Attempts - 2026-06-24

## Purpose

This records what was attempted while trying to build approved kitchen-sink sections as native Webflow Designer elements, what worked partially, what failed, which skills were loaded, and which docs were not reviewed.

The target work was not a mass builder. The request was to help add individual approved sections into Webflow, one section at a time, then document each section back to Airtable.

## Scope

Target Webflow site:

- `ringstatus`
- site id: `6982268b7543ac3c80151266`

Target pages used:

- `kitchen-sink`
- page id: `6a3c0f785a7ed7e425d31d51`
- `kitchen-sink2`
- page id: `6a3c7873ec35c16b737f6289`

Primary source HTML:

- `C:\Users\gombc\.codex\attachments\51a445ef-2cc5-4aed-97bf-7f421ccb10b9\pasted-text.txt`

Airtable documentation table:

- base: `rscom`
- base id: `appDN3R51ZPmwgMib`
- table: `rs_section_inventory`
- table id: `tbldwSqcbQ2JEVup7`

## Current State Created By These Attempts

No repo files were changed before this document.

No Airtable records were updated during the failed build attempts.

Webflow Designer changes made during failed tests:

- A bad partial `rs_blog_index_v1` section was created on `kitchen-sink`:
  - element id: `9ffa35d1-99ee-7c76-fc88-93eee4ccbc81`
  - issue: top section classes existed, but nested child wrapper/card classes were not preserved.
- A native builder test section was created on `kitchen-sink2`:
  - element id: `2db43fd6-e34c-3591-9820-6ae819112874`
  - issue: `element_builder` created the section but did not apply requested style names/classes.

These should be treated as test/partial artifacts, not approved sections.

## What Was Tried

### Attempt 1: `whtml_builder` with full section HTML

Target:

- `rs_blog_index_v1`

Method:

- Inserted source HTML using `mcp__webflow_beta.whtml_builder`.
- Added tracking attribute:
  - `data-rs-section-key="rs_blog_index_v1"`

Observed result:

- Insert succeeded.
- Returned top element:
  - element id: `9ffa35d1-99ee-7c76-fc88-93eee4ccbc81`
  - type: `Section`
  - styleNames: `rs-section`, `is-wildcard`, `is-blog-index`
- Nested children returned with empty `styleNames`.

Evidence:

- First child wrappers that should have had classes like `rs-wildcard-sticky` and `rs-wildcard-scroll-layer` came back as `styleNames: []`.

Status:

- Failed as a reliable method.

Reason:

- It can insert HTML, but it did not preserve nested Webflow class bindings.
- This is not acceptable for approved native kitchen-sink sections.

### Attempt 2: Targeted verification after a timeout

Method:

- Queried by `data-rs-section-key="rs_blog_index_v1"` after a timed-out insert.

Observed result:

- The query did not filter reliably.
- It returned hundreds of elements instead of only the matching section.

Status:

- Failed as a clean verification method.

Reason:

- It could not prove whether a duplicate section existed.
- Broad output was too noisy for safe mutation decisions.

### Attempt 3: Select exact old section and snapshot

Target:

- existing `rs_blog_index_v1` element:
  - `904f9936-02d7-fcd2-06b5-4fe49537ee58`

Method:

- `element_tool.select_element`
- `element_snapshot_tool`

Observed result:

- Exact section selection worked.
- Snapshot worked.
- Snapshot confirmed the old/existing `rs_blog_index_v1` section rendered incorrectly.

Status:

- Worked for inspection.
- Not a build method.

Reason:

- Useful for verifying exact elements.
- Does not solve creating native sections.

### Attempt 4: `element_builder` with nested section schema

Target:

- clean test on `kitchen-sink2`

Method:

- Used `mcp__webflow_beta.element_builder`.
- First schema used `Block` as child type.

Observed result:

- Failed validation.
- Tool expected specific enum values such as:
  - `Section`
  - `DivBlock`
  - `Heading`
  - `Paragraph`
  - etc.

Status:

- Failed before creating anything.

Reason:

- Wrong schema enum.

### Attempt 5: `element_builder` with valid `DivBlock` enum

Target:

- clean test on `kitchen-sink2`

Method:

- Retried with:
  - `Section`
  - nested `DivBlock`
  - nested `DivBlock`

Observed result:

- Section was created:
  - element id: `2db43fd6-e34c-3591-9820-6ae819112874`
- Returned section had `styleNames: []`.

Status:

- Partially worked.
- Not reliable as a direct classed-section build method.

Reason:

- It created native elements.
- It did not apply the requested classes/style names from the schema.

### Attempt 6: `element_tool.set_style` using raw class names

Target:

- test section:
  - `2db43fd6-e34c-3591-9820-6ae819112874`

Method:

- Tried to apply:
  - `rs-section`
  - `is-native-builder-test`

Observed result:

- Tool rejected raw class/style names.
- Error:
  - `One or more styles not found: rs-section, is-native-builder-test`

Status:

- Failed.

Reason:

- `set_style` appears to require valid Webflow style references or existing style records in the correct format, not arbitrary raw class strings.

### Attempt 7: `style_tool.query_styles`

Target:

- lookup existing style records for:
  - `rs-section`
  - `is-hero`
  - `rs-section-container`
  - `rs-section-padding`

Method:

- `mcp__webflow_beta.style_tool`

Observed result:

- Timed out after approximately 60 seconds.
- Error indicated Webflow Designer MCP app should be checked/restarted/foregrounded.

Status:

- Failed due MCP timeout.

Reason:

- Could not resolve style records needed to test `set_style` correctly.

## What Worked

Worked for inspection:

- `de_page_tool.get_current_page`
- `de_page_tool.switch_page`
- `element_tool.select_element` by exact element id
- `element_snapshot_tool` by exact element id
- `element_tool.get_all_elements` on `kitchen-sink2`, although it produced very large output and should not be used repeatedly on large pages

Worked partially:

- `whtml_builder` inserted a top-level section and preserved top-level classes.
- `element_builder` created native elements when valid enum values were used.

Did not work reliably enough:

- `whtml_builder` for full approved sections because nested classes were dropped.
- `element_builder` as a direct classed-section builder because requested style names were not applied.
- `set_style` with raw names because style references were rejected.
- `style_tool.query_styles` because it timed out.

## Skills Loaded

Loaded and read:

- `webflow-mcp:designer-tools`
  - path: `C:\Users\gombc\.agents\skills\webflow-mcp-designer-tools\SKILL.md`
  - relevant instruction: use Webflow MCP Designer tools; `element_builder` supports max 3 levels per call.
- `webflow-mcp:flowkit-naming`
  - path: `C:\Users\gombc\.agents\skills\webflow-mcp-flowkit-naming\SKILL.md`
  - relevant instruction: naming/class structure guidance; did not solve MCP class application.
- `webflow-mcp:custom-code-management`
  - path: `C:\Users\gombc\.agents\skills\webflow-mcp-custom-code-management\SKILL.md`
  - relevant instruction: custom-code management only; not the desired native Designer element path.
- `webflow-cli:code-component`
  - path: `C:\Users\gombc\.agents\skills\webflow-cli-code-component\SKILL.md`
  - relevant instruction: Code Components are a separate React/component deployment path; not used for this native section insertion.
- `airtable:airtable-cli`
  - path: `C:\Users\gombc\.codex\plugins\cache\openai-curated\airtable\6fe38d4f\skills\airtable-cli\SKILL.md`
  - relevant instruction: Airtable CLI can update records by field ids; not used after Webflow verification failed.

## Skills Available But Not Loaded

Not loaded in the section-build attempts:

- `webflow-cli:cloud`
- `webflow-mcp:safe-publish`
- `build-web-apps:frontend-testing-debugging`
- `product-design:audit`
- `superpowers:systematic-debugging`
- `superpowers:verification-before-completion`
- `superpowers:using-superpowers`

## Superpowers Tried

None were loaded or executed during the Webflow section-build attempt.

This was a process miss. Given repeated Webflow MCP failures, `superpowers:systematic-debugging` should have been loaded before continuing repeated section attempts.

## MCP Docs Reviewed

Reviewed:

- Local Webflow skill docs listed above.

Not reviewed:

- Official Webflow MCP documentation.
- Official Webflow Designer MCP API reference for `element_builder`.
- Official Webflow Designer MCP API reference for `whtml_builder`.
- Official Webflow Designer MCP API reference for `style_tool`.
- Official Webflow Designer MCP FAQ for timeouts, except the timeout message itself linked:
  - `https://developers.webflow.com/mcp/v1.0.0/reference/fa-qs#the-mcp-server-is-timing-out-in-the-designer`

Also not used:

- `webflow_guide_tool`

Reason:

- The current callable namespace exposed `mcp__webflow_beta` tools but not `webflow_guide_tool`.
- This was stated, but no official-doc fallback was performed before continuing.

## Known Tooling Issues

### WHTML class preservation issue

`whtml_builder` preserved top-level section classes but dropped nested child classes.

Impact:

- Cannot use it for approved kitchen-sink sections without proving a way to preserve/apply nested classes.

### Element builder class issue

`element_builder` created native elements but did not apply the requested `styleNames`.

Impact:

- Cannot assume schema `styleNames` becomes Webflow classes.

### Style application issue

`element_tool.set_style` rejected raw class names.

Impact:

- Need exact style reference format or style ids before class application can be proven.

### Style lookup timeout

`style_tool.query_styles` timed out.

Impact:

- Could not prove style lookup/apply path.

### Broad element reads are too heavy

`get_all_elements` can return very large page trees.

Impact:

- It is useful once for discovery on small or clean pages.
- It should not be repeatedly used as the normal verification method on dense pages.

## What Should Not Be Repeated

Do not continue using `whtml_builder` for full approved kitchen-sink sections until nested class preservation is proven.

Do not assume `element_builder.styleNames` applies classes.

Do not use raw class strings with `set_style` unless the correct payload shape is proven.

Do not keep retrying `style_tool.query_styles` after timeout without the user reopening/foregrounding MCP Bridge or approving a changed method.

Do not update Airtable section records until the Webflow section is verified.

## Current Recommended Gate Before More Section Work

Before building more sections, one of these must be true:

1. A Webflow-native method is proven to create one section with correct nested classes and verified by exact element ids.
2. The user builds the section manually in Designer and the assistant only verifies/document IDs/classes back to Airtable.
3. A different approved production method is chosen, such as static/embedded HTML, reusable Webflow components, or Webflow Code Components.

## Minimal Evidence Required For A Section To Count As Done

For each section:

- Webflow page id confirmed.
- New/approved top section element id captured.
- Top section has expected base and modifier classes.
- Key child wrappers have expected classes.
- Visible snapshot confirms section resembles source.
- `rs_section_inventory` record updated with:
  - `webflow_page_id`
  - `webflow_element_id`
  - `data_rs_section_key`
  - `source_template`
  - `approval_status`
  - notes indicating verification state

## Bottom Line

The Webflow MCP path used in this run did not prove reliable for building approved native kitchen-sink sections.

The correct next action is not to continue section production through the same method. The next action must either prove the missing class-application step with official docs/tool evidence, or switch to a manual/native Designer build plus Airtable documentation workflow.
